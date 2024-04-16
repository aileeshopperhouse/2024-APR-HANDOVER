c = NaverCrawler()

# 반려동물
ctgs = {}
now_level = 1
now_selector = 0

sleep = random.uniform(5, 7)

# 반려동물용품 아래 15개 ul
ul1 = c.driver.find_elements(
                    By.XPATH,
                    f"""//*[name()='ul' and contains(@class, 'filter_finder_list__')]
                    //*[name()='li']
                    //*[name()='a' and contains(@role, 'button')]"""
                )
for i in tqdm(range(0, len(ul1), 1)):
    ul1 = c.driver.find_elements(
                    By.XPATH,
                    f"""//*[name()='ul' and contains(@class, 'filter_finder_list__')]
                    //*[name()='li']
                    //*[name()='a' and contains(@role, 'button')]"""
    )
    # 펫가전
    ul1[i].click()
    time.sleep(sleep)
#     id1 = u1.id


    string = c.driver.find_elements(
                        By.XPATH,
                        f"""//*[name()='h4' and contains(@class, 'filter_finder_cell_tit__')]
                        //*[name()='a' and contains(@role, 'button')]"""
                    )[0].text

    string = re.sub(r"[0-9]", "", string)
#     ctgs['string'] = c.driver.current_url

    active_ctg = c.driver.find_elements(
                    By.XPATH,
                    f"""//*[name()='ul' and contains(@class, 'filter_finder_list__')]
                    //*[name()='li' and contains(@class, 'filter_active__')]
                    //*[name()='a' and contains(@role, 'button')]"""
    )
    
    if(active_ctg != []):
        print('end')
        ctgs[string] = c.driver.current_url
        
        reset = c.driver.find_elements(
        By.XPATH,
        f"""//*[name()='h4' and contains(@class, 'filter_finder_cell_tit__')]
        //*[name()='a' and contains(@role, 'button')]""")

        reset[0].click()
        time.sleep(sleep)
        
        continue
    else:
        pass
    
    # 드라이룸, 급수기, 정수기
    ul2 = c.driver.find_elements(
                    By.XPATH,
                    f"""//*[name()='ul' and contains(@class, 'filter_finder_list__')]
                    //*[name()='li']
                    //*[name()='a' and contains(@role, 'button')]"""
                )
    
    for j in tqdm(range(0, len(ul2), 1)):
        ul2 = c.driver.find_elements(
                    By.XPATH,
                    f"""//*[name()='ul' and contains(@class, 'filter_finder_list__')]
                    //*[name()='li']
                    //*[name()='a' and contains(@role, 'button')]"""
                )
        ul2[j].click()
        time.sleep(sleep)
        
        string = c.driver.find_elements(
                        By.XPATH,
                        f"""//*[name()='h4' and contains(@class, 'filter_finder_cell_tit__')]
                        //*[name()='a' and contains(@role, 'button')]"""
                    )[0].text

        string = re.sub(r"[0-9]", "", string)
    #     ctgs['string'] = c.driver.current_url

        active_ctg = c.driver.find_elements(
                    By.XPATH,
                    f"""//*[name()='ul' and contains(@class, 'filter_finder_list__')]
                    //*[name()='li' and contains(@class, 'filter_active__')]
                    //*[name()='a' and contains(@role, 'button')]"""
    )

        if(active_ctg != []):
            print('end')
            ctgs[string] = c.driver.current_url
            reset = c.driver.find_elements(
            By.XPATH,
            f"""//*[name()='h4' and contains(@class, 'filter_finder_cell_tit__')]
            //*[name()='a' and contains(@role, 'button')]""")

            reset[0].click()
            time.sleep(sleep)
            continue
        else:
            pass
        
        
        ul3 = c.driver.find_elements(
                    By.XPATH,
                    f"""//*[name()='ul' and contains(@class, 'filter_finder_list__')]
                    //*[name()='li']
                    //*[name()='a' and contains(@role, 'button')]"""
                )
    
        for k in tqdm(range(0, len(ul3), 1)):
            ul3 = c.driver.find_elements(
                        By.XPATH,
                        f"""//*[name()='ul' and contains(@class, 'filter_finder_list__')]
                        //*[name()='li']
                        //*[name()='a' and contains(@role, 'button')]"""
                    )
            ul3[k].click()
            time.sleep(sleep)

            string = c.driver.find_elements(
                            By.XPATH,
                            f"""//*[name()='h4' and contains(@class, 'filter_finder_cell_tit__')]
                            //*[name()='a' and contains(@role, 'button')]"""
                        )[0].text

            string = re.sub(r"[0-9]", "", string)
        #     ctgs['string'] = c.driver.current_url

            active_ctg = c.driver.find_elements(
                        By.XPATH,
                        f"""//*[name()='ul' and contains(@class, 'filter_finder_list__')]
                        //*[name()='li' and contains(@class, 'filter_active__')]
                        //*[name()='a' and contains(@role, 'button')]"""
        )

            if(active_ctg != []):
                print('end')
                ctgs[string] = c.driver.current_url
                reset = c.driver.find_elements(
                By.XPATH,
                f"""//*[name()='h4' and contains(@class, 'filter_finder_cell_tit__')]
                //*[name()='a' and contains(@role, 'button')]""")

                reset[0].click()
                time.sleep(sleep)
                continue
            else:
                pass
            
        
        reset = c.driver.find_elements(
        By.XPATH,
        f"""//*[name()='h4' and contains(@class, 'filter_finder_cell_tit__')]
        //*[name()='a' and contains(@role, 'button')]""")

        reset[0].click()
        time.sleep(sleep)
        
        
    
    reset = c.driver.find_elements(
    By.XPATH,
    f"""//*[name()='h4' and contains(@class, 'filter_finder_cell_tit__')]
    //*[name()='a' and contains(@role, 'button')]""")
    
    reset[0].click()
    time.sleep(sleep)