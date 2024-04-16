import pandas as pd
import requests
import time

from user_agent import generate_user_agent

proxies = {
    'http': 'socks5://127.0.0.1:9050',
    'https': 'socks5://127.0.0.1:9050',
}

headers = {'User-Agent': generate_user_agent(device_type = 'desktop'),
            # 'referer': 'https://search.shopping.naver.com/search/category/100000697?',
           'cookie':None,
            # 'cookie': 'NNB=F2CHAAKR7JGGG; ASID=01d6306400000184739f32430000005c; m_loc=719c730f761e2ca7954652acab9d64e0442273ce5d02607a5f541a44ebdf5cbc; NV_WETR_LOCATION_RGN_M="MDk2ODAxMDQ="; NV_WETR_LAST_ACCESS_RGN_M="MDk2ODAxMDQ="; _ga=GA1.2.808872344.1707785291; _ga_KF0FS2B56F=GS1.2.1707785291.1.0.1707785291.0.0.0',
        #    'cookie': """NNB=F2CHAAKR7JGGG; autocomplete=use; ASID=01d6306400000184739f32430000005c; m_loc=719c730f761e2ca7954652acab9d64e0442273ce5d02607a5f541a44ebdf5cbc; NV_WETR_LOCATION_RGN_M="MDk2ODAxMDQ="; NV_WETR_LAST_ACCESS_RGN_M="MDk2ODAxMDQ="; viewType=list; _fwb=243MT7PoM9YvOeLtcRT8VZi.1703561892636; _ga=GA1.2.808872344.1707785291; _ga_KF0FS2B56F=GS1.2.1707785291.1.0.1707785291.0.0.0; SHP_BUCKET_ID=7; spage_uid=; ncpa=95694|luhqlpk0|d7c11d0bb45750c9c0ed98812a3761808dcacee1|95694|aa1dd49cb8033b7314d8587fbb960108b521f7f7:10274030|luhqqfb4|219679ce4e7afc720a96acbee3eb8759f0e042ed|s_db3e035cfe71|d5d5f0dffce79360cdd87d51fcfcdbd03680fddd""",
        #    'x-aut-web-t': '420x66695ztde3qao6a69',
#            'authority': 'search.shopping.naver.com',
#            'method': 'GET',
            'sbth': "a135045efb84de1f38c3ee38c6fa84ba42c7f060a91b3d2245da54a4674071768324fc5b3e7ab015f63a77d066ba7aee"#"bdb615e7289417d34b15e0f6d7c5112e2754c20e04c1b4dc9376b358c0c344a3151d04d69d0f0a368b7a6e85ba7d0d3a",
#            'accept': 'application/json, text/plain, */*',
#            'accept-encoding': 'gzip, deflate, br',
#            'accept-language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
            }

print(headers)
# url = "https://search.shopping.naver.com/api/search/category/100000697?catId=50000155&eq=&iq=%EA%B0%95%EC%95%84%EC%A7%80&pagingIndex=2&pagingSize=40&productSet=model&sort=rel&viewType=list&window=&xq="

url = "https://shopping.naver.com/home"

with requests.Session() as s:
    response = s.get(url, headers = headers, proxies=proxies, timeout = (10, 10))
    print(response)
    print(s.cookies)

    url = "https://shopping.naver.com/logistics/home"
    referer = "https://shopping.naver.com/home"

    headers['referer'] = referer

    response = s.get(url, headers = headers, proxies=proxies, timeout = (10, 10))
    print(response)
    print(s.cookies).