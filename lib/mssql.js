const Dump = require('./dump');
const Blob = require('node-blob');

const SQL = {
	listTables:
		"SELECT TABLE_NAME AS [name] FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA = [table_schema]",
	listColumns:
		'SELECT TABLE_NAME AS [tableName], ORDINAL_POSITION AS [position], COLUMN_NAME AS [name] FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = [table_schema]',
	listConstraints: `SELECT tc.TABLE_NAME AS foreignTable, tc2.TABLE_NAME AS primaryTable
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
            ON tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
        INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc2
            ON rc.UNIQUE_CONSTRAINT_NAME = tc2.CONSTRAINT_NAME
        WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
		`,
	listIdentityTables:
		"SELECT TABLE_NAME AS [name] FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMNPROPERTY(OBJECT_ID(TABLE_SCHEMA+'.'+TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1",
	listComputedColumns: 'SELECT [name] FROM sys.columns WHERE is_computed = 1 AND object_id = OBJECT_ID(?)',
	setIdentityOn: 'SET IDENTITY_INSERT ??.?? ON',
	setIdentityOff: 'SET IDENTITY_INSERT ??.?? OFF'
};

// AND tc.CONSTRAINT_SCHEMA = ?

class MSSQLDump extends Dump {
	tables() {
		return Promise.all([
			this.knex.raw(SQL.listTables, this.config.schema),
			this.knex.raw(SQL.listColumns, this.config.schema),
			this.knex.raw(SQL.listConstraints, this.config.schema)
		]).then(([tables, columns, constraints]) => {
			let toSort = tables.map(({ name }) => ({
				name,
				order: [columns.find(c => c.tableName === name && c.position === 1).name]
			}));
			let result = [];
			while (toSort.length > 0) {
				// sort tables with the less dependents first
				const nonDependents = toSort.filter(t => !constraints.find(c => c.primaryTable === t.name));
				result.push(...nonDependents);
				toSort = toSort.filter(t => !nonDependents.some(d => d === t));
				constraints = constraints.filter(c => !result.some(r => c.foreignTable === r.name));
			}

			return result;
		});
	}

	columnCount(rows, table) {
		const computedColumns = this.computedColumns[table];
		return Object.keys(rows[0]).filter(k => !computedColumns.includes(k)).length;
	}

	removeComputedColumns(table, chunk) {
		return chunk.map(e =>
			Object.keys(e).reduce(
				(res, k) => (!this.computedColumns[table].includes(k) ? { ...res, [k]: e[k] } : res),
				{}
			)
		);
	}

	insert(trx, table, chunk) {
		chunk = this.removeComputedColumns(table, chunk);

		if (this.identityTables.includes(table)) {

			const handleChunk = (chunk) => {

				if(chunk[0].logo) {
					const array = new Uint8Array(chunk[0].logo.data);

					const blob = new Blob([array]);

					return [{...chunk[0],  ...(chunk[0].logo.data ? {logo: blob} : {logo: null})}]
				} else {
					return chunk
				}

			}

			// this table has an identity column
			const on = trx.raw(SQL.setIdentityOn, ["dbo", table]).toSQL();
			const insert = trx(table)
			// .insert(chunk)
				 .insert(handleChunk(chunk))
				.toSQL();
			const off = trx.raw(SQL.setIdentityOff, ["dbo", table]).toSQL();


			// table has an identity column,
			// insert needs to be done with IDENTITY_INSERT ON
			// and that needs to run in the same query as the insert
			return trx.raw(`${on.sql}; ${insert.sql}; ${off.sql};`, [
				...on.bindings,
				...insert.bindings,
				...off.bindings
			]);
		} else {


			return super.insert(trx, table, chunk);
		}
	}

	async restore(data) {
		// list tables
		const tables = data.getTables();


		// set identity tables
		this.identityTables = await this.knex.raw(SQL.listIdentityTables).then(tables => tables.map(t => t.name));


		// set computed columns
		this.computedColumns = await Promise.all(
			tables.map(table => this.knex.raw(SQL.listComputedColumns, table))
		).then(results =>
			results.reduce(
				(res, computedColumns, ix) => ({ ...res, [tables[ix]]: computedColumns.map(cc => cc.name) }),
				{}
			)
		);

		// then restore
		return super.restore(data);
	}
}

module.exports = MSSQLDump;
