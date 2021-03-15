import {Client} from "pg";

const config = {
    user: 'testuser',
    host: '127.0.0.1',
    database: 'testdb',
    password: 'testpass',
    port: 5433,
}

const main = async () => {
    const client = new Client(config);
    await client.connect();

    await client.query(`SET search_path = ag_catalog, "$user", public;`);
    // await client.query(`SELECT create_graph('testgraph');`);

    await client.query(
        `SELECT * FROM cypher('testgraph', $$MATCH (v) return v$$) as (a agtype);`
    ).catch(e => console.log(e));

    await client.query(
        `SELECT * FROM cypher('testgraph', $$ CREATE (v:Person {name: $1}) $$) as (a agtype);`,
        [
            'Tom'
        ]
    ).catch(e => console.log(e));
}

main()
