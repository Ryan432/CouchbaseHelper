const _ = require('lodash');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const buildLimitQuery = (limit) => {
	const isValidLimit = _.isFinite(limit) && limit >= 0;
	if (!isValidLimit) {
		throw new TypeError('Limit selector is not valid.');
	}
	return `LIMIT ${limit}`;
};

const buildOffsetQuery = (offset) => {
	const isValidOffset = _.isFinite(offset) && offset >= 0;
	if (!isValidOffset) {
		throw new TypeError('Offset selector is not valid.');
	}
	return `OFFSET ${offset}`;
};

const parseSetOperatorValue = (setOperator, setKey, setValue) => {
	switch (setOperator) {
		case '||':
			return `${setKey} = ${setKey} || ${parseQueryValue(setValue)}`;
		case 'array_remove':
			return `${setKey} = ${buildArrayRemoveQuery(setKey, setValue)}`;
		case '#-':
			return `${setKey} = ${setKey} #- ${parseQueryValue(setValue)}`;
		default:
			throw new TypeError(`The operator '${operator}' is not supported.`);
	}
};

const escapeValue = (value) => {
	// TODO: Make sure escaping also protects from SQL injection attacks
	return _.replace(value, /'/g, `''`);
};

const parseQueryValue = (value) => {
	switch (typeof value) {
		case 'object': {
			if (_.isNull(value)) {
				return value;
			}
			const isPostgresArray = _.isArray(value) && !_.every(value, (item) => _.isPlainObject(item));
			return isPostgresArray ? `ARRAY[${_.map(value, (item) => parseQueryValue(item))}]` : `'${escapeValue(JSON.stringify(value))}'`;
		}
		case 'string':
			return `'${escapeValue(value)}'`;
		case 'number':
		case 'boolean':
			return value;
		default:
			throw new TypeError(`The type '${typeof value}' is not supported.`);
	}
};

const parseQueryOperatorValue = (operator, value) => {
	switch (operator) {
		case 'BETWEEN':
		case 'NOT BETWEEN':
			return `${parseQueryValue(_.first(value))} AND ${parseQueryValue(_.last(value))}`;
		case 'ILIKE':
		case 'LIKE':
		case 'NOT ILIKE':
		case 'NOT LIKE':
			return `'%${escapeValue(value)}%'`;
		case 'IN':
		case 'NOT IN':
			return `(${_.map(value, (item) => parseQueryValue(item))})`;
		case 'IS':
		case 'NOT IS':
			return value;
		case '=':
		case '!=':
		case '<':
		case '<=':
		case '>':
		case '>=':
		case '@>':
		case '<@':
		case '&&':
		case 'NOT &&':
			return parseQueryValue(value);
		default:
			throw new TypeError(`The operator '${operator}' is not supported.`);
	}
};

const buildWhereQuery = (where) => {
	const isValidWhere = _.isArray(where) && where.length > 0 && _.every(where, (whereItem) => _.isPlainObject(whereItem));
	if (!isValidWhere) {
		throw new TypeError('Where selector is not valid.');
	}
	const whereConditions = _.map(where, (whereItem) => {
		const itemConditions = _.map(whereItem, (whereItemValue, whereItemKey) => {
			const queryOperator = _.isPlainObject(whereItemValue) ? whereItemValue.operator : _.isArray(whereItemValue) ? 'IN' : '=';

			const queryValue = _.isPlainObject(whereItemValue) ? whereItemValue.value : whereItemValue;

			if (_.includes(queryOperator, 'NOT')) {
				return `NOT (${whereItemKey} ${_.replace(queryOperator, 'NOT ', '')} ${parseQueryOperatorValue(queryOperator, queryValue)})`;
			}

			return `${whereItemKey} ${queryOperator} ${parseQueryOperatorValue(queryOperator, queryValue)}`;
		});
		return `(${_.join(itemConditions, ' AND ')})`;
	});

	return `WHERE ${_.join(whereConditions, ' OR ')}`;
};

const selectQueryBuilder = ({ bucketName, selector: { columns, where, limit, offset } }) => {
	const whereQuery = _.isNil(where) || _.size(where) === 0 ? '' : buildWhereQuery(where);
	const limitQuery = _.isNil(limit) ? '' : buildLimitQuery(limit);
	const offsetQuery = _.isNil(offset) ? '' : buildOffsetQuery(offset);
	return `SELECT ${_.size(columns) > 0 ? columns : '*'} FROM \`${bucketName}\` ${whereQuery} ${limitQuery} ${offsetQuery}`;
};

// const queryObject = {
// 	bucketName: 'users',
// 	selector: {
// 		// columns: ['age', 'firstName', 'id', 'lastName', 'type'],
// 		// where: [
// 		// 	{
// 		// 		age: {
// 		// 			operator: '>',
// 		// 			value: 10
// 		// 		}
// 		// 	}
// 		// ]
// 		limit: 1
// 	}
// };

// console.log(selectQueryBuilder(queryObject));

const updateQueryBuilder = () => {};

const queryBuilder = ({ bucketName, columns, selector, returning }, queryType) => {
	let query;

	switch (queryType) {
		case 'select':
			query = selectQueryBuilder({ bucketName, selector });
			break;
		// case 'insert':
		// 	query = insertQueryBuilder({ tableSchema, tableName, queryUser, columns, returning });
		// 	break;
		// case 'update':
		// 	query = updateQueryBuilder({ tableSchema, tableName, queryUser, columns, selector, returning });
		// 	break;
		// case 'delete':
		// 	query = deleteQueryBuilder({ tableSchema, tableName, selector, returning });
		// 	break;
		// case 'createTable':
		// 	query = createTableQueryBuilder({ tableSchema, tableName, columns });
		// 	break;
		default:
			throw new TypeError('Unsupported query type.');
	}

	return {
		query
	};
};
module.exports = { sleep, selectQueryBuilder, updateQueryBuilder, queryBuilder };
