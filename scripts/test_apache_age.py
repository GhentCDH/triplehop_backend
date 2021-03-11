import asyncio
import asyncpg
import psycopg2

from databases import Database


async def main1():
    pool = await asyncpg.create_pool('postgresql://crdb:crdb@127.0.0.1:5432/crdb', statement_cache_size=0)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                '''
                    SET search_path = ag_catalog, "$user", public;
                '''
            )

            transaction = conn.transaction()
            try:
                await transaction.start()
                await conn.execute(
                    '''
                    SELECT * FROM cypher('h', $$MATCH (v) return v$$) as (a agtype);
                    '''
                )
            except asyncpg.exceptions.InternalServerError:
                await transaction.rollback()
            else:
                print('unhandled cypher(cstring) function call error no longer occurring')

            # result = await conn.fetchrow(
            #     '''
            #         SELECT * FROM cypher('h', $$MATCH (v) return v$$) as (a agtype);
            #     '''
            # )
            # print(result)

            await conn.execute(
                '''
                    SELECT * FROM cypher('h', $TAG$CREATE (v:Part {id: '$sf'})$TAG$) as (a agtype);
                ''',
                # 6,
            )


async def main2():
    async with Database(url='postgresql://crdb:crdb@127.0.0.1:5432/crdb', statement_cache_size=0) as db:
        async with db.transaction():
            await db.execute(
                '''
                    SET search_path = ag_catalog, "$user", public;
                '''
            )

            transaction = db.transaction()
            try:
                await transaction.start()
                await db.execute(
                    '''
                    SELECT * FROM cypher('h', $$MATCH (v) return v$$) as (a agtype);
                    '''
                )
            except asyncpg.exceptions.InternalServerError:
                await transaction.rollback()
            else:
                print('unhandled cypher(cstring) function call error no longer occurring')

            await db.execute_many(
                '''
                    SELECT * FROM cypher('h', $$CREATE (v:Part {id: :id})$$) as (a agtype);
                ''',
                [
                    {
                        'id': 5,
                    }
                ],
            )
            # print([r[1] for r in result[0]])
            # print(result[0]._column_map_int)


def main3():
    conn = psycopg2.connect("dbname=crdb user=crdb host=127.0.0.1 port=5432 password=crdb")

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                '''
                    SET search_path = ag_catalog, "$user", public;
                '''
            )
            try:
                cur.execute(
                    '''
                        SELECT * FROM cypher('h', $$MATCH (v) return v$$) as (a agtype);
                    '''
                )
            except Exception:
                pass

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                '''
                    SET search_path = ag_catalog, "$user", public;
                '''
            )
            cur.execute(
                '''
                    SELECT * FROM cypher(%(graph)s, $TAG$CREATE (v:Part {id: %(id)s})$TAG$) as (a agtype);
                ''',
                {
                    'graph': 'h',
                    'id': 9,
                }
            )
            print(cur.mogrify(
                '''
                    SELECT * FROM cypher(%(graph)s, $TAG$CREATE (v:Part {id: %(id)s})$TAG$) as (a agtype);
                ''',
                {
                    'graph': 'h',
                    'id': 9,
                }
            ))


# main3()

asyncio.get_event_loop().run_until_complete(main3())
