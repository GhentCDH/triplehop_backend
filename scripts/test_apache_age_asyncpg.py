import asyncio
import asyncpg


async def main():
    # Don't use prepared statements (see https://github.com/apache/incubator-age/issues/28)
    pool = await asyncpg.create_pool('postgresql://testuser:testpass@127.0.0.1:5432/testdb', statement_cache_size=0)
    async with pool.acquire() as conn:
        await conn.execute(
            '''
                SET search_path = ag_catalog, "$user", public;
            '''
        )

        # await conn.execute(
        #     '''
        #         SELECT create_graph('testgraph');
        #     '''
        # )

        transaction = conn.transaction()
        try:
            await transaction.start()
            await conn.execute(
                '''
                    SELECT * FROM cypher('testgraph', $$MATCH (v) return v$$) as (a agtype);
                '''
            )
        except asyncpg.exceptions.InternalServerError:
            await transaction.rollback()
        else:
            print('unhandled cypher(cstring) function call error no longer occurring')

        await conn.execute(
            '''
                SELECT * FROM cypher('testgraph',$$ CREATE (:a_b {name: 'Tom'})$$) as (a agtype);
                SELECT * FROM cypher('testgraph',$$ CREATE (:a_b {name: 'Jane'})$$) as (a agtype);
            ''',
        )

asyncio.get_event_loop().run_until_complete(main())
