#필요 라이브러리 다운
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import pickle
import sys, os
from tqdm import tqdm
import time
import asyncio
import tracemalloc
import gc

#함수 및 URL 정의
def get_results(endpoint_url, query):
    user_agent = "MyCoolTool/0.1 foo@example.org"
    sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()

endpoint_url = "https://query.wikidata.org/sparql"


if 'triple.pkl' not in os.listdir('Round2'):
    #table 데이터
    table_dir = "Round2/Tables_Round2/tables"
    filelist = os.listdir(table_dir)
    data={}
    for file in tqdm(filelist):
        data[file[:-4]] = pd.read_csv(table_dir+"/"+file, encoding="UTF-8", dtype=str)


    #table을 triple 형식으로 나타내기
    spo_set = []
    for table in data:
        for row in data[table].values:
            for i in range(1,len(row)):
                spo_set.append([row[0],table+"_col{}".format(i),row[i]])

    with open('Round2/triple.pkl', 'wb') as f:
        pickle.dump(spo_set,f)


#asyncio를 활용한 query 검색

with open('Round2/triple.pkl', 'rb') as f:
    spo_set = pickle.load(f)
    print(len(spo_set))
    iter_num = len(spo_set) // 10000 + 1


async def main():
    tracemalloc.start()
    def get_results_async(p, query):
        propertylist = []
        try:
            results = get_results(endpoint_url, query)
            for result in results["results"]["bindings"]:
                propertylist.append(result['property']['value'])
            return (p, propertylist)

        except:
            return (p, ["error"])

    def result_return(spo):
        s, p, o = spo
        # query1 : object-property
        query1 = "\"".join(["""SELECT ?subject ?property ?object 
                            WHERE
                            {
                            ?subject rdfs:label|schema:description|skos:altLabel """ ,str(s), """@en.
                            ?object rdfs:label|schema:description|skos:altLabel """ ,str(o), """@en. 
                            ?subject ?property ?object. 
                            SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'. } 
                            }
                            """])

        # query2_1 : data-property(str or date)
        query2_1 = "\"".join(["""SELECT ?subject ?property ?object 
                            WHERE 
                            { 
                            ?subject rdfs:label|schema:description|skos:altLabel """ ,str(s), """@en.
                            {{?subject ?property """, str(o), """@en. } # Data type이 str인 경우
                            UNION
                            {?subject ?property """, str(o), """^^xsd:dateTime. }} # Data type이 날짜 데이터인 경우
                            SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'. } 
                            }"""])

        # query2_2 : data-property(num)
        query2_2 = """SELECT ?subject ?property ?object 
                            WHERE 
                            { 
                            ?subject rdfs:label|schema:description|skos:altLabel """ ,str(s), """@en.
                            ?subject ?property ?object.
                            FILTER(?object = """+ str(o)+ """).  
                            SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'. } 
                            }
                            """
        # query3 : partition of data
        query3_1 = "\"".join(["""SELECT distinct ?subject ?property ?object 
                            WHERE {
                            {
                            ?subject rdfs:label|schema:description|skos:altLabel ?s.
                            ?object rdfs:label|schema:description|skos:altLabel """ ,str(o), """@en. 
                            ?subject ?property ?object.
                            filter(regex(lcase(str(?s)),""", str(s).lower(), """))
                            SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'. }
                            }
                            UNION
                            {
                            ?subject rdfs:label|schema:description|skos:altLabel """, str(s), """@en.
                            ?object rdfs:label|schema:description|skos:altLabel ?o.
                            ?subject ?property ?object.
                            filter(regex(lcase(str(?o)),""", str(o).lower(), """))
                            SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'. }
                            }
                            }
                            """])
        query3_2 = """SELECT distinct ?subject ?property 
                            WHERE 
                            { 
                            ?subject rdfs:label|schema:description|skos:altLabel ?s. 
                            ?subject ?property """+str(o)+""". 
                            FILTER (regex(lcase(str(?s)) , \""""+str(s).lower()+"""\")) 
                            SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'. } 
                            }"""

        query3_3 = """SELECT distinct ?subject ?property 
                            WHERE 
                            { 
                            ?subject rdfs:label|schema:description|skos:altLabel ?s. 
                            {?subject ?property \""""+str(o)+"""\"@en.} 
                            UNION
                            {?subject ?property \""""+str(o)+"""\"^^xsd:dateTime.} 
                            FILTER (regex(lcase(str(?s)) , \""""+str(s).lower()+"""\")) 
                            SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'. } 
                            }"""

        # query4 : partition of data 2
        query4_1 = "\"".join(["""SELECT distinct ?subject ?property ?object 
                            WHERE {
                            {
                            ?subject rdfs:label|schema:description|skos:altLabel ?s.
                            ?object rdfs:label|schema:description|skos:altLabel """ ,str(o), """@en. 
                            ?subject ?property ?object.
                            filter(regex(""", str(s).lower(), """, lcase(str(?s))))
                            SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'. }
                            }
                            UNION
                            {
                            ?subject rdfs:label|schema:description|skos:altLabel """, str(s), """@en.
                            ?object rdfs:label|schema:description|skos:altLabel ?o.
                            ?subject ?property ?object.
                            filter(regex(""", str(o).lower(), """,lcase(str(?o))))
                            SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'. }
                            }
                            }
                            """])
        query4_2 = """SELECT distinct ?subject ?property 
                            WHERE 
                            { 
                            ?subject rdfs:label|schema:description|skos:altLabel ?s. 
                            ?subject ?property """+str(o)+""". 
                            FILTER (regex(\""""+str(s).lower()+"""\",lcase(str(?s)))) 
                            SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'. } 
                            }"""

        query4_3 = """SELECT distinct ?subject ?property 
                            WHERE 
                            { 
                            ?subject rdfs:label|schema:description|skos:altLabel ?s. 
                            {?subject ?property \""""+str(o)+"""\"@en.} 
                            UNION
                            {?subject ?property \""""+str(o)+"""\"^^xsd:dateTime.} 
                            FILTER (regex(\""""+str(s).lower()+"""\", lcase(str(?s)))) 
                            SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'. } 
                            }"""

        # task 1,2,3을 실행
        result = get_results_async(p, query1)
        query = "1"
        if result[1] == [] or result[1] == ["error"]:
            result = get_results_async(p, query2_1)
            query = "2_1"
        if result[1] == [] or result[1] == ["error"]:
            result = get_results_async(p, query2_2)
            query = "2_2"
        if result[1] == [] or result[1] == ["error"]:
            result = get_results_async(p, query3_1)
            query = "3_1"
        if result[1] == [] or result[1] == ["error"]:
            result = get_results_async(p, query3_2)
            query = "3_2"
        if result[1] == [] or result[1] == ["error"]:
            result = get_results_async(p, query3_3)
            query = "3_3"
        if result[1] == [] or result[1] == ["error"]:
            result = get_results_async(p, query4_1)
            query = "4_1"
        if result[1] == [] or result[1] == ["error"]:
            result = get_results_async(p, query4_2)
            query = "4_2"
        if result[1] == [] or result[1] == ["error"]:
            result = get_results_async(p, query4_3)
            query = "4_3"
        return [result, "query {}".format(query)]


    #각 triple별 단위 task 지정
    async def property_query(semaphore, spo):
        async with semaphore:
            nonlocal num, running_num, query_num
            running_num += 1
            num += 1
            result = await loop.run_in_executor(None,result_return,spo)
            #상태표시용
            print("current iter : {0}    running_num : {1}        result : {2}".format(num, running_num, result), flush=True)
            '''if num % 100 == 0:
                print(num)
            else:
                print(".", end="")'''
            running_num -=1
            await asyncio.sleep(1)
            return result



    for iter in range(iter_num):
        if 'Round2/Demo_result{}.pkl'.format(iter) not in os.listdir():
            print("current iter: {}".format(iter))
            begin = time.time()
            #동시수행 query 수 5개(wikidata client 준수사항에 근거)
            sem = asyncio.BoundedSemaphore(5)
            running_num=0
            num = 1
            query_num = 0
            #task 목록을 지정하는 futures
            futures = [asyncio.ensure_future(property_query(sem, spo)) for spo in spo_set[iter*10000:min((iter+1)*10000,len(spo_set))]]

            #task들을 비동기적으로 실행
            answer = await asyncio.gather(*futures)

            with open('Round2/Demo_result{}.pkl'.format(iter), 'wb') as f:
                pickle.dump(answer,f)

            del futures
            del answer

            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics('lineno')

            print("[ Top 10 ]")
            for stat in top_stats[:10]:
                print(stat)
            end = time.time()
            print('실행 시간: {0:.3f}초'.format(end - begin))

            gc.collect()



loop = asyncio.get_event_loop()  # 이벤트 루프를 얻음
loop.run_until_complete(main())  # main이 끝날 때까지 기다림
loop.close()  # 이벤트 루프를 닫음
