#필요 라이브러리 다운
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import pickle
import sys, os
from tqdm import tqdm
import time
import asyncio

#함수 및 URL 정의
def get_results(endpoint_url, query):
    user_agent = "MyCoolTool/0.1 foo@example.org"
    sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()

endpoint_url = "https://query.wikidata.org/sparql"

#table 데이터
table_dir = "Tables_Round1/tables"
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


#asyncio를 활용한 query 검색

async def main():

    def result_return(spo, query):
        s, p, o = spo
        propertylist = []
        try:
            results = get_results(endpoint_url, query)
            for result in results["results"]["bindings"]:
                propertylist.append(result['property']['value'])
            return (p, propertylist)

        except:
            error_list.append([s, p, o])
            return (p, ["error"])

    #각 triple별 단위 task 지정
    async def property_query(semaphore, spo):
        s, p, o = spo
        async with semaphore:
            nonlocal num, error_list, query_num

            #상태표시용
            if num % 100 == 0:
                print(num)
            else:
                print(".", end="")
            num += 1

            #query1 : object-property
            query1 ="""SELECT ?subject ?property ?object 
                    WHERE
                    {
                    ?subject rdfs:label \"""" + str(s) + """\"@en. 
                    ?object rdfs:label \"""" + str(o) + """\"@en. 
                    ?subject ?property ?object. 
                    SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'. } 
                    }
                    """

            #query2_1 : data-property(str or date)
            query2_1="""SELECT ?subject ?property ?object 
                    WHERE 
                    { 
                    ?subject rdfs:label \"""" + str(s) + """\"@en.  
                    {{?subject ?property \"""" + str(o) + """\"@en. } # Data type이 str인 경우
                    UNION
                    {?subject ?property \"""" + str(o) + """\"^^xsd:dateTime. }} # Data type이 날짜 데이터인 경우
                    SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'. } 
                    }
                    """

            #query2_2 : data-property(num)
            query2_2="""SELECT ?subject ?property ?object 
                    WHERE 
                    { 
                    ?subject rdfs:label \"""" + str(s) + """\"@en.
                    ?subject ?property ?object.
                    FILTER(?object = """+str(o)+""").  
                    SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'. } 
                    }
                    """

            #query3 : partition of data
            query3= """
                    SELECT distinct ?subject ?property ?object 
                    WHERE {
                    {
                    ?subject rdfs:label ?s.
                    ?object rdfs:label \"""" + str(o) + """\"@en. 
                    ?subject ?property ?object.
                    filter(regex(str(?s),\"""" + str(s) + """\"))
                    SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'. }
                    }
                    UNION
                    {
                    ?subject rdfs:label \"""" + str(s) + """\"@en.
                    ?object rdfs:label ?o.
                    ?subject ?property ?object.
                    filter(regex(str(?o),\"""" + str(o) + """\"))
                    SERVICE wikibase:label { bd:serviceParam wikibase:language 'en'. }
                    }
                    }
                    """

            #task 1,2,3을 실행
            result = result_return(spo, query1)
            query = "1"
            query_num+=1
            if result[1]==[] or result[1] ==["error"]:
                result = result_return(spo, query2_1)
                query = "2_1"
                query_num += 1
            if result[1]==[] or result[1] ==["error"]:
                result = result_return(spo, query2_2)
                query = "2_2"
                query_num += 1
            if result[1]==[] or result[1] ==["error"]:
                result = result_return(spo, query3)
                query = "3"
                query_num += 1


            return [result, "query {}".format(query)]

    with open('Demo_result.pkl', 'wb') as f:

        #동시수행 query 수 5개(wikidata client 준수사항에 근거)
        sem = asyncio.BoundedSemaphore(5)
        error_list=[]
        num = 1
        query_num = 0
        #task 목록을 지정하는 futures
        test_num=100 #예시로 보여주기 위한 개수
        futures = [asyncio.run_in_executor(property_query(sem, spo)) for spo in spo_set[:100]]

        #task들을 비동기적으로 실행
        answer = await asyncio.gather(*futures)
        print("실행 쿼리 수: {}".format(query_num))
        pickle.dump(answer,f)

if "Demo_result.pkl" not in os.listdir():
    begin = time.time()
    loop = asyncio.get_event_loop()  # 이벤트 루프를 얻음
    loop.run_until_complete(main())  # main이 끝날 때까지 기다림
    loop.close()  # 이벤트 루프를 닫음
    end = time.time()
    print('실행 시간: {0:.3f}초'.format(end - begin))

with open('Demo_result.pkl', 'rb') as f:
    demo=pickle.load(f)
    for n, row in enumerate(demo):
        print(spo_set[n],row)