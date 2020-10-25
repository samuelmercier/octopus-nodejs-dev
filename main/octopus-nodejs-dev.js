
const fs=require("fs");
const http=require("http");
const mysql=require("mysql");

//create user 'octopus'@'%' identified with mysql_native_password by 'octopus';
//create database octopus;
//grant all on octopus.* to 'octopus'@'%';

/** this exception is typically meaningful to the end-user (e.g. duplicate record, value out of allowed range, ...). */
class BusinessLogicException extends Error {

	constructor(status, message) {
		super(message);
		this.status=status;
	}

}

/** mimic of its java.lang counterpart: the arguments passed to a method invocation cannot produce a valid result. */
class IllegalArgumentException extends Error {

	constructor(message) { super(message); }

}

/** the execution engine reached an invalid state (e.g. insert outside of a transaction).
	this exception is typically meant to be displayed to a developer.
*/
class InvalidStateException extends Error {

	constructor(message, cause) {
		super(message);
		this.cause=cause;
	}

}

const Type=Object.freeze({
	instantiateFromDescriptor:Object.freeze(function(descriptor) {
		switch(descriptor) {
		case "boolean":
			return Type.BOOLEAN;
		case "date":
			return Type.DATE;
		case "datetime":
			return Type.DATETIME;
		case "int":
			return Type.INT;
		case "string":
			return Type.STRING;
		case "uuid":
			return Type.UUID;
		}
		if(descriptor.startsWith("string(")) {
			const closeParenthesisIndex=descriptor.indexOf(")", 7);
			if(closeParenthesisIndex===-1)
				throw new IllegalArgumentException("invalid string descriptor.");
			const length=Number(descriptor.substring(7, closeParenthesisIndex));
			if(Number.isNaN(length))
				throw new IllegalArgumentException("invalid string descriptor.");
			if(length===-1)
				return Type.STRING;
			if(length<=-2)
				throw new IllegalArgumentException("invalid string descriptor: negative length.");
			return Type.instantiateString(length);
		}
		throw new IllegalArgumentException("invalid descriptor.");
	}),
	instantiateString:Object.freeze(function(length) {
		return Object.freeze({
			kind:"string",
			length:length,
			stringToValue:function(value) {
				if(value.length>length)
					throw new IllegalArgumentException("length overflow (actual "+value.length+" exeeds maximal "+length+").");
				return value;
			}
		});
	}),
	BOOLEAN:Object.freeze({
		kind:"boolean",
		stringToValue:function(string) {
			switch(string) {
			case "false":
				return false;
			case "true":
				return true;
			default:
				throw new IllegalArgumentException("invalid boolean '"+string+"'.");
			}
		},
		valueToString:function(value) { return value ? "true" : "false"; }
	}),
	DATE:Object.freeze({
		kind:"date",
		stringToValue:function(string) {
			if(string.length!==10||string[4]!=='-'||string[7]!=='-')
				throw new IllegalArgumentException("invalid date '"+string+"'.");
			const result=Date.parse(string);
			if(Number.isNaN(result))
				throw new IllegalArgumentException("invalid date '"+string+"'.");
			return new Date(result);
		},
		valueToString:function(value) { return value.toISOString(); }
	}),
	DATETIME:Object.freeze({
		kind:"datetime",
		stringToValue:function(string) {
			if(string.length!==19||string[4]!=='-'||string[7]!=='-'||string[10]!=='T'||string[13]!==':'||string[16]!==':')
				throw new IllegalArgumentException("invalid datetime '"+string+"'.");
			const result=Date.parse(string);
			if(Number.isNaN(result))
				throw new IllegalArgumentException("invalid datetime '"+string+"'.");
			return new Date(result);
		},
		valueToString:function(value) { return value.toISOString(); }
	}),
	INT:Object.freeze({
		kind:"int",
		stringToValue:function(string) {
			const result=Number(string);
			if(Number.isNaN(result)||result<Number.MIN_SAFE_INTEGER||result>Number.MAX_SAFE_INTEGER)
				throw new IllegalArgumentException("invalid int '"+string+"'.");
			return result;
		},
		valueToString:function(value) { return value.toString(); }
	}),
	STRING:Object.freeze({
		kind:"string",
		length:-1,
		stringToValue:function(string) { return string; },
		valueToString:function(value) { return value; }
	}),
	UUID:Object.freeze({
		kind:"uuid",
		stringToValue:function(string) { throw new Error("not implemented yet."); },
		valueToString:function(value) { return value; }
	})
});

function instantiateHttpModule() {

	function instantiateModuleContext(executionContext) {

		async function executePlain(statement) {
			const payload=[];
			if(statement.body!==undefined)
				for(const fragment of statement.body) {
					if(fragment.startsWith(":")) {
						const variable=executionContext.getVariable(fragment.substring(1));
						payload.push(variable.type.valueToString(variable.value));
						continue;
					}
					payload.push(fragment);
				}
			if(statement.headers!==undefined)
				for(const header of statement.headers)
					executionContext.response.setHeader(header.name, header.value);
			executionContext.response.writeHead(statement.status);
			executionContext.response.end(payload.join(""));
		}

		return Object.freeze({
			"executeStatement":async function(statement) {
				switch(statement.kind) {
				case "plain":
					return executePlain(statement);
				default:
					throw new Error("invalid mysql statement kind '"+statement.kind+"'.");
				}
			},
			"finalize":async()=>new Promise((resolve, reject)=>resolve(undefined))
		});
	}

	return Object.freeze({
		"name":"http",
		"instantiateModuleContext":instantiateModuleContext,
		"finalize":async()=>undefined
	});
}

async function instantiateMysqlModule(descriptor) {

	function instantiateModuleContext(executionContext) {

		async function execute(body) {
			const sql=body
				.map(fragment=>{
					if(typeof fragment==="string")
						return fragment;
					return variableToMysql(executionContext.getVariable(fragment.substring(1)));
				})
				.join("");
			if(connection===null)
				connection=await new Promise((resolve, reject)=>{
					pool.getConnection((error, connection)=>error===null ? resolve(connection) : reject(error));
				});
			console.info("mysql execute "+sql);
			return new Promise((resolve, reject)=>{
				connection.query(sql, (error, results, fields)=>{
					error===null ? resolve({ fields:fields, rows:results }) : reject(error);
				});
			});
		}

		function variableToMysql(variable) {
			switch(variable.type.kind) {
			case "boolean":
				return variable.value ? "true" : "false";
			case "date":
				return "STR_TO_DATE("+mysql.escape(variable.value.toISOString())+", '%Y-%m-%dT%T.%fZ')";
			case "datetime":
				return "STR_TO_DATE("+mysql.escape(variable.value.toISOString())+", '%Y-%m-%dT%T.%fZ')";
			case "int":
				return variable.value;
			case "string":
				return mysql.escape(variable.value);
			case "uuid":
				return "UUID_TO_BIN("+mysql.escape(variable.value)+")";
			default:
				throw new IllegalArgumentException("cannot convert variable '"+variable.name+"' to mysql: unknown type kind '"+variable.type.kind+"'.");
			}
		}

		async function executeAggregate(statement) {
			const { fields, rows }=await execute(statement.query.body);
			switch(rows.length) {
			case 0:
				executionContext.response.setHeader("Content-Type", "text/plain");
				executionContext.response.writeHead(404);
				executionContext.response.end("Empty.");
				return;
			case 1:
				executionContext.response.setHeader("Content-Type", "application/json");
				executionContext.response.writeHead(200);
				const result={};
				const clauses=statement.query.clauses;
				for(let clauseIndex=0; clauseIndex<clauses.length; clauseIndex+=1)
					result[clauses[clauseIndex].name]=rows[0][fields[clauseIndex].name];
				for(const subquery of statement.subqueries)
					result[subquery.name]=await executeAggregateSubquery(subquery);
				executionContext.response.end(JSON.stringify(result));
				return;
			default:
				throw new InvalidStateException("Main aggregate query returned more than a single row.");
			}
		}

		async function executeAggregateSubquery(subquery) {
			const result=[];
			const { fields, rows }=await execute(subquery.body);
			const clauses=subquery.clauses;
			const clauseCount=clauses.length;
			for(const row of rows) {
				const current={};
				for(let clauseIndex=0; clauseIndex<clauseCount; clauseIndex+=1)
					current[clauses[clauseIndex].name]=row[fields[clauseIndex].name];
				result.push(current);
			}
			return result;
		}

		async function executeAssign(statement) {
			const { fields, rows }=await execute(statement.query);
			if(rows.length===0)
				throw new InvalidStateException("empty resultset.");
			const type=Type.instantiateFromDescriptor(statement.type);
			executionContext.assignVariable(statement.assignName, type, type.stringToValue(rows[0][fields[0].name]));
		}

		async function executeCheck(statement) {
			if(statement.requireTransaction===false&&!transactionStarted)
				throw new InvalidStateException("executing 'CHECK' outside of a transaction.");
			const { fields, rows }=await execute(statement.query);
			if(rows.length!==0) {
				await execute([ "ROLLBACK" ]);
				transactionStarted=false;
				throw new BusinessLogicException(409, rows[0][fields[0].name]);
			}
		}

		async function executeUpdate(statement) {
			if(!transactionStarted)
				throw new InvalidStateException("executing 'UPDATE' outside of a transaction.");
			const status=(await execute(statement.query)).rows;
			if(statement.affectedRowsName!==undefined) {
				console.info("octopus setting '"+statement.affectedRowsName+"' to affectedRows ("+status.affectedRows+").");
				executionContext.assignVariable(statement.affectedRowsName, Type.INT, status.affectedRows);
			}
			if(statement.changedRowsName!==undefined) {
				console.info("octopus setting '"+statement.changedRowsName+"' to changedRows ("+status.changedRows+").");
				executionContext.assignVariable(statement.changedRowsName, Type.INT, status.changedRows);
			}
		}

		let connection=null;
		let transactionStarted=false;
		return Object.freeze({
			"executeStatement":async function(statement) {
				switch(statement.kind) {
				case "aggregate":
					return executeAggregate(statement);
				case "assign":
					return executeAssign(statement);
				case "check":
					return executeCheck(statement);
				case "commit":
					if(!transactionStarted)
						throw new InvalidStateException("executing 'COMMIT' outside of a transaction.");
					await execute([ "COMMIT" ]);
					transactionStarted=false;
					return;
				case "rollback":
					if(!transactionStarted)
						throw new InvalidStateException("executing 'ROLLBACK' outside of a transaction.");
					await execute([ "ROLLBACK" ]);
					transactionStarted=false;
					return;
				case "startTransaction":
					if(transactionStarted)
						throw new InvalidStateException("executing 'START TRANSACTION' inside of a transaction.");
					await execute([ "START TRANSACTION" ]);
					transactionStarted=true;
					return;
				case "update":
					return executeUpdate(statement);
				default:
					throw new Error("invalid mysql statement kind '"+statement.kind+"'.");
				}
			},
			"finalize":async function() {
				if(transactionStarted) {
					await execute([ "ROLLBACK" ]);
					throw new InvalidStateException("started transaction was neither committed nor rollbacked.");
				}
				if(connection!==null)
					connection.release();
			}
		});
	}

	const pool=mysql.createPool({
		"connectionLimit":descriptor.connectionLimit||10,
		"database":descriptor.database,
		"host":descriptor.host,
		"user":descriptor.user,
		"password":descriptor.password,
		"port":descriptor.port,
		"insecureAuth":true
	});
	const result=Object.freeze({
		"name":descriptor.name,
		"instantiateModuleContext":instantiateModuleContext,
		"finalize":async()=>new Promise((resolve, reject)=>pool.end(error=>error===undefined ? resolve(undefined) : reject(error)))
	});
	return new Promise((resolve, reject)=>resolve(result));
}

class OctopusEngine {

	static async execute(request, response, descriptor) {
		const engine=new OctopusEngine(descriptor);
		try {
			await engine.initialize();
			await engine.execute(request, response);
		} finally {
			await engine.finalize();
		}
	}

	constructor(descriptor) {
		this.descriptor=descriptor;
		this.modules=Object.create(null);
		Object.freeze(this);
	}

	async initialize() {
		const names=new Set([ "http" ]);
		this.modules.http=instantiateHttpModule();
		if(this.descriptor.modules!==undefined) {
			for(const descriptor of this.descriptor.modules) {
				if(names.has(descriptor.name))
					throw new Error("redefinition of module '"+descriptor.name+"'.");
				names.add(descriptor.name);
				this.modules[descriptor.name]=await this.instantiateModule(descriptor);
			}
		}
	}

	async instantiateModule(descriptor) {
		switch(descriptor.className) {
		case "mysql":
			return instantiateMysqlModule(descriptor);
		default:
			throw new Error("cannot instantiate module '"+moduleName+"': unknown class '"+descriptor.className+"'.");
		}
	}

	async instantiateExecutionContext(request, response) {
		const variables=Object.create(null);
		const result=Object.freeze({
			moduleContexts:Object.create(null),
			request:request,
			response:response,
			assignVariable:function(name, type, value) {
				if(variables[name]!==undefined)
					throw new InvalidStateException("reassignment of variable '"+name+"'.");
				console.info("octopus assigning '"+name+"' to '"+value+"'.");
				variables[name]=Object.freeze({ "type":type, "value":value });
			},
			getVariable:function(name) {
				const result=variables[name];
				if(result===undefined)
					throw new InvalidStateException("cannot resolve variable '"+name+"'.");
				return result;
			}
		});
		for(const moduleName in this.modules)
			result.moduleContexts[moduleName]=await this.modules[moduleName].instantiateModuleContext(result);
		Object.freeze(result.moduleContexts);
		for(const variableName in this.descriptor.variables) {
			const variable=this.descriptor.variables[variableName];
			const type=Type.instantiateFromDescriptor(variable.type);
			result.assignVariable(variableName, type, type.stringToValue(variable.value));
		}
		return result;
	}

	async execute(request, response) {
		const executionContext=await this.instantiateExecutionContext(request, response);
		try {
			for(const statement of this.descriptor.statements) {
				const moduleContext=executionContext.moduleContexts[statement.module];
				if(moduleContext===undefined)
					throw new Error("cannot resolve module '"+statement.module+"'");
				await moduleContext.executeStatement(statement);
			}
		} catch(e) {
			if(e instanceof BusinessLogicException) {
				response.setHeader("Content-Type", "application/json");
				response.writeHead(e.status);
				return void response.end(JSON.stringify({ status:e.status, message:e.message }));
			}
			console.error(e);
			response.setHeader("Content-Type", "text/plain");
			response.writeHead(500);
			response.end(e.toString());
		}
		for(const moduleName in executionContext.moduleContexts)
			try {
				await executionContext.moduleContexts[moduleName].finalize();
			} catch(e) {
				console.error(e);
			}
	}

	async finalize() {
		for(const moduleName in this.modules)
			try {
				await this.modules[moduleName].finalize();
			} catch(e) {
				console.error("octopus finalization of module '"+moduleName+"' failed:");
				console.error(e);
			}
	}

}

http.createServer(function(request, response) {
	switch(request.method) {
	case "GET": {
		const payload=fs.readFileSync("main/octopus-nodejs-dev.html");
		response.setHeader("Content-Type", "text/html; charset=utf-8");
		response.writeHead(200);
		response.end(payload);
		return;
	}
	case "POST": {
		const payload=[];
		request.on("data", data=>payload.push(data.toString("utf-8")));
		request.on("end", ()=>OctopusEngine.execute(request, response, JSON.parse(payload.join(""))));
		return;
	}
	default:
		response.setHeader("Content-Type", "text/plain");
		response.writeHead(405);
		response.end("Unsupported method.");
		return;
	}
}).listen(8080);

