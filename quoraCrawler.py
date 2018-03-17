from selenium import webdriver
from bs4 import BeautifulSoup
import time
import os
import numpy as np
from getproxy import getproxy
import hashlib
from selenium.common.exceptions import TimeoutException

import pandas as pd

page_count = 0
max_page_count = 50

PROXY = ''
DEBUG = 0

#TODO: Determine method to prevent deviating from root topic ex. I started with machine learning and ended at
#TODO: Bipolar disorder, one way being control the max_depth
#Usage: increase wandering time to gather more questions from each topic
#TODO: check and Update user crawling
#TODO: move browser to backend:done using headless version of chrome driver
#TODO: Dynamically check if there is throtling and switch among proxies : using randomized proxies every n pages
#TODO: use hash instead of data everywhere ;using md5:
#system might so different behavior using py 3+ due to randomization of hash function
#TODO: create pandas tables as discussed
proxyDF = None


def crawlTopicHierarchy():
    global page_count,PROXY,proxyDF
    page_count = 0
    if (DEBUG): print "In crawlTopicHierarchy()..."
    max_time_to_wander = 200

    #Create stack to keep track of links to visit and visited
    urls_to_visit = []
    urls_visited = []
    q_urls_visited = []

    # print type(proxyDF)
    proxyObj = getproxy()
    proxyDF = proxyObj.getProxy()
    PROXY = proxyDF.loc[page_count,'ip'] + ":" + proxyDF.loc[page_count,'port']
    print 'using PROXY ',PROXY


    #search till second level depth for non unique urls
    max_depth = 7
    related_topics = None
    # Create files for topic names and topic URLs
    file_topic_urls = open("topic_urls.csv", mode='w')
    file_question_urls = open("question_urls.csv", mode='w')


    # Starting node link
    url = 'http://www.quora.com/topic/Machine-learning'

    depth = 0
    # topic_names_hierarchy = ""

    # Add root to stack
    urls_to_visit.append([url, depth])
    urls_visited.append(hashlib.md5(url.encode('utf-8')).hexdigest())
    # print urls_visited[0]

    #if (DEBUG): print urls_to_visit

    while len(urls_to_visit):
        # Pop stack of stack to get URL and current depth
        url, current_depth = urls_to_visit.pop()
        if (DEBUG): print 'Current url:{0} current depth:{1} depth:{2}'.format(url, str(current_depth), str(depth))

        topic = url[27:]
        if (DEBUG): print topic

        depth += 1
        # Record topic URL
        file_topic_urls.write((url + '####' + hashlib.md5(url.encode('utf-8')).hexdigest() + '\n').encode('utf-8'))

        url_about = url

        #grab a new proxy if the page count has exceeded
        if page_count > max_page_count:
            #reset page-count
            page_count = 0
            proxyObj = getproxy()
            proxyDF = proxyObj.getProxy()
            PROXY = proxyDF.loc[page_count,'ip']+":"+proxyDF.loc[page_count,'port']
            print 'using PROXY ',PROXY

        chromedriver = "chromedriver"   # Needed?
        os.environ["webdriver.chrome.driver"] = chromedriver    # Needed?
        options = webdriver.ChromeOptions()
        ##chrome options to run in background i.e headless version
        # PROXY = '165.227.115.60:8118'
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--proxy-server=http://%s' % PROXY)
        # options.add_argument('--window-size=1200,1100')

        # options.add_argument("--silent")
        # options.add_argument("--disable-dev-tools")
        # options.add_argument('--remote-debugging-port=9222')
        # options.add_argument('--window-size=1280,1696')

        # browser = webdriver.Chrome(chrome_options=options)
        # browser.set_page_load_timeout(20)

        # browser = webdriver.Chrome()

        # browser.get("http://whatismyipaddress.com")
        #elliminate slow loading proxies
        while 1:
            try:
                options.add_argument('--headless')
                options.add_argument('--disable-gpu')
                options.add_argument('--proxy-server=http://%s' % PROXY)
                # browser.quit()
                browser = webdriver.Chrome(chrome_options=options)
                browser.set_page_load_timeout(30)
                browser.get(url_about)
                break
            except TimeoutException:
                browser.quit()
                page_count += 1
                PROXY = proxyDF.loc[page_count, 'ip'] + ":" + proxyDF.loc[page_count, 'port']
                print 'using PROXY ', PROXY
                continue
        time.sleep(1)

        # Fetch the hover item, dont scroll down without fetching this imprtant item
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight/6);")
        src_updated = browser.page_source

        soup = BeautifulSoup(src_updated, 'html5lib')
        related_topics = soup.find_all('a', attrs={"class": "RelatedTopicsListItem HoverMenu"})

        refresh_count=0
        #use refresh counter to reset proxy
        Proxy_refreshed = False
        while(not related_topics) and (depth<max_depth):
            # time_to_spend = np.random.randint(1,4)
            time_to_spend = np.random.randint(2,6)
            time.sleep(time_to_spend)
            src_updated = browser.page_source
            soup = BeautifulSoup(src_updated, 'html5lib')
            related_topics = soup.find_all('a', attrs={"class": "RelatedTopicsListItem HoverMenu"})
            if not related_topics:
                try:
                    browser.execute_script("location.reload();")
                except TimeoutException:
                    browser.quit()
                    page_count += 1
                    PROXY = proxyDF.loc[page_count, 'ip'] + ":" + proxyDF.loc[page_count, 'port']
                    print 'using PROXY ', PROXY

                    options.add_argument('--headless')
                    options.add_argument('--disable-gpu')
                    options.add_argument('--proxy-server=http://%s' % PROXY)
                    browser = webdriver.Chrome(chrome_options=options)
                    browser.set_page_load_timeout(30)
                    print 'using PROXY ', PROXY
                    Proxy_refreshed=True
                    continue

            if refresh_count > 5 and not related_topics and not Proxy_refreshed:
                Proxy_refreshed=False
                refresh_count = 0
                browser.quit()
                page_count += 1
                PROXY = proxyDF.loc[page_count, 'ip'] + ":" + proxyDF.loc[page_count, 'port']

                options.add_argument('--headless')
                options.add_argument('--disable-gpu')
                options.add_argument('--proxy-server=http://%s' % PROXY)
                browser = webdriver.Chrome(chrome_options=options)
                browser.set_page_load_timeout(30)
                print 'using PROXY ', PROXY

            print related_topics
            print 'refreshed'
            refresh_count += 1


        print 'fetched and moving on!'
        src = ""

        q_marker = 'question_link'
        topic_marker = 'TopicNameLink'

        #get related topics from Hover card
        for topic in related_topics:
            # print topic.span.string
            try:
                topic_url = "http://www.quora.com" + r'/topic/'+topic.span.string.replace(' ','-')\
                            .replace('(','').replace(')','').replace('.','')
                topicHash = hashlib.md5(topic_url.encode('utf-8')).hexdigest()
                if topicHash not in urls_visited:
                    #print topic_url,topicHash
                    file_topic_urls.write((topic_url + '####' + topicHash + '\n').encode('utf-8'))
                    urls_to_visit.append([topic_url, depth])
                    urls_visited.append(topicHash)

            except:
                continue

        total_time = 0
        #getting the page source for a given topic
        #this might need change if the page source moves out stuff out of the visible window
        while src != src_updated and total_time < max_time_to_wander:
            ##crawl at a random rate from 1 to 5 #wait for more time at the start
            time_to_spend = np.random.randint(2,6)
            time.sleep(time_to_spend)
            total_time += time_to_spend
            src = src_updated
            #THis is working :Scroll to bottom of page
            #https://stackoverflow.com/questions/12792236/sent-user-to-page-bottom-by-javascript
            browser.execute_script("window.scrollTo(0, document.body.scrollHeight*0.80);")
            src_updated = browser.page_source

        src_updated = browser.page_source
        soup = BeautifulSoup(src_updated, 'html5lib')

        #get the question url and decide if or not before writing to disk
        for all_classes in soup.find_all('a'):
            cl_text = all_classes.get('class')
            #use either chromedrive with Selenium or beutifull soup..Using both is confusing
            if cl_text is not None and cl_text[0] is not None and q_marker in cl_text[0]:
                # print cl_text
                # print question.get_text()
                question = all_classes.get('href')
                q_url = "http://www.quora.com" + question
                qHash = hashlib.md5(q_url.encode('utf-8')).hexdigest()
                if qHash not in q_urls_visited:
                    #print q_url
                    q_urls_visited.append(qHash)
                    file_question_urls.write((q_url + '####' + qHash + '####'\
                                              + hashlib.md5(url_about.encode('utf-8')).hexdigest() + '\n').encode('utf-8'))

            #writing garbage, needs to be fixed?
            # if cl_text is not None and cl_text[0] is not None and topic_marker in cl_text[0]:
            #     # print cl_text
            #     # print question.get_text()
            #     topic = all_classes.get('href')
            #     topic_url = "http://www.quora.com" + topic
            #     # topics to visit, start appending to the list with depth value, controlled by max depth
            #     topicHash = hashlib.md5(topic_url.encode('utf-8')).hexdigest()
            #     if topicHash not in urls_visited:
            #         print topic_url
            #         file_topic_urls.write((topic_url + '\n').encode('utf-8'))
            #         urls_to_visit.append([topic_url, current_depth])
            #         urls_visited.append(topicHash)

        browser.quit()
        page_count += 1
    # File cleanup
    file_topic_urls.close()
    file_question_urls.close()
    return urls_visited

# Crawl a question URL and save data into a csv file
def crawlQuestionData(file):
    global page_count,PROXY,proxyDF
    if (DEBUG): print ("In crawlQuestionData...")
    
    # Open question url file
    file_data = open(file+'out', mode = 'w')

    q_hash = []
    topic_hash = []
    q_count = 0
    max_q_count = 500
    q_stack = []
    max_time_to_wander = 60

    #generate unique q ids' stack
    with open(file, 'rU') as file_question_urls:
        for q_url in file_question_urls:


            if q_url.split('####')[0] not in q_stack and q_url.split('####')[0].find('http://www.quora.com/unanswered/') == -1:
                q_stack.append(q_url.split('####')[0])
                #print(q_url)
                q_hash.append(q_url.split('####')[1])
                topic_hash.append(q_url.split('####')[2])

    #iterate through the question stack
    while (q_stack and q_count<max_q_count):
        if page_count > max_page_count or page_count == 0:
            # reset page-count
            page_count = 0
            proxyObj = getproxy()
            proxyDF = proxyObj.getProxy()
            proxyDF = proxyDF[proxyDF['code'].str.contains('US')].reset_index()
            PROXY = proxyDF.loc[page_count, 'ip'] + ":" + proxyDF.loc[page_count, 'port']
            print 'using PROXY ', PROXY

        current_link = q_stack.pop()
        if (DEBUG): print "Current question URL:", current_link
        page_count += 1

        # grab a new proxy if the page count has exceeded
        if page_count > max_page_count:
            proxyObj = getproxy()
            proxyDF = proxyObj.getGoodIP(proxyObj.getProxy(), countryCode='US')
            PROXY = proxyDF['ip'] + ":" + proxyDF['port']
            print 'using PROXY ', PROXY

        # Open browser to current_question_url
        chromedriver = "chromedriver"  # Needed?
        os.environ["webdriver.chrome.driver"] = chromedriver  # Needed?
        options = webdriver.ChromeOptions()
        ##chrome options to run in background i.e headless version
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--proxy-server=http://%s' % PROXY)
        # options.add_argument('--window-size=1200,1100')
        browser = webdriver.Chrome(chrome_options=options)
        browser.set_page_load_timeout(30)

        while 1:
            try:
                browser.get(current_link)
                # time_to_spend = np.random.randint(2, 6)
                time_to_spend = np.random.randint(1)
                # time.sleep(time_to_spend)
                browser.execute_script("window.scrollTo(0, document.body.scrollHeight );")
                break
            except TimeoutException:
                browser.quit()
                page_count += 1
                # PROXY = proxyDF.loc[page_count, 'ip'] + ":" + proxyDF.loc[page_count, 'port']
                options.add_argument('--headless')
                options.add_argument('--disable-gpu')
                #options.add_argument('--proxy-server=http://%s' % PROXY)
                browser = webdriver.Chrome(chrome_options=options)
                browser.set_page_load_timeout(30)
                print 'using PROXY ', PROXY
                continue


        if (DEBUG): print 'loaded page...'
        total_time = 0
        src = ''
        src_updated = browser.page_source

        # while src != src_updated and total_time<max_time_to_wander:
        while src != src_updated:
            ##crawl at a random rate from 1 to 5
            #wait for more time at the start
            time_to_spend = np.random.randint(1,6)
            time.sleep(time_to_spend)
            # total_time += time_to_spend
            src = src_updated
            browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            src_updated = browser.page_source

        if (DEBUG): print 'updated source'
        # src_updated = browser.page_source
        elem = browser.find_element_by_xpath("//*")
        src_updated = elem.get_attribute("outerHTML")
        soup = BeautifulSoup(src_updated,'html5lib')

        ans_class = 'answer_'
        allAnswerTags = soup.find_all('div', class_="AnswerHeader ContentHeader".split())
        # if (DEBUG):print allAnswerTags
        i_max = allAnswerTags.__len__()
        for i, answer in enumerate(allAnswerTags):
            # print answer
            if (DEBUG): print 'ITERATOR', i
            i += 1
            # answer_id = answer.findNext('div', class_="AnswerHeader ContentHeader".split())
            answer_id = answer
            # print 'cl_text',answer_id
            try:
                if answer_id is not None:
                    prof_tag = answer_id.findNext('a')
                    if prof_tag is not None:
                        p_url = prof_tag.get('href')
                        prof_name_t = prof_tag.findNext('img')
                    else:
                        p_url = 'empty'
                        prof_name_t = 'No-Node'
                    if (DEBUG): print p_url

                    if prof_name_t is not None:
                        p_name = prof_name_t.get('alt')
                        prof_cred_basic = prof_name_t.findNext('span',
                                                               class_="IdentityNameCredential NameCredential".split())
                    else:
                        p_name = 'Anon'
                        prof_cred_basic = None

                    if (DEBUG): print p_name

                    if prof_cred_basic is not None:
                        p_cred = prof_cred_basic.get_text()
                        ans_tag = prof_cred_basic.findNext('span', class_="rendered_qtext")
                    else:
                        p_cred = 'No-affiliation'
                    if (DEBUG): print p_cred

                    p_ans = ans_tag.get_text()
                    if p_ans is None:
                        p_ans = ' '
                    if (DEBUG): print p_ans

                    # missing credential ,grab the next answer
                    if i < i_max:
                        answer_id_next = allAnswerTags[i]
                        if answer_id_next is not None:
                            ans_tag_next = answer_id_next.findNext('span', class_="rendered_qtext")
                        if ans_tag_next is not None:
                            p_ans_next = ans_tag_next.get_text()
                            if p_ans == p_ans_next:
                                ans_tag = prof_name_t.findNext('span', class_="rendered_qtext")
                                if ans_tag is not None:
                                    p_cred = 'No-affiliation'
                                    p_ans = ans_tag.get_text()

                    upvote_tag = ans_tag.findNext('a', class_="VoterListModalLink AnswerVoterListModalLink".split())
                    if upvote_tag is not None:
                        p_upvt = upvote_tag.get_text()
                    else:
                        p_upvt = '0'
                    if (DEBUG): print p_upvt

                    if (DEBUG == 2): print 'FINAL eXTRACT', p_url, ' ', p_name, ' ', p_cred, ' ', p_ans

                    file_data.write(
                        (p_url + '####' + p_name + '####' + p_cred + '####' + p_ans + '####' + p_upvt + '####' + \
                         q_hash.pop() + '####' + topic_hash.pop()).encode('utf8'))
            except:
                if (DEBUG): print 'catching some exception'
                continue

        browser.quit()
        q_count += 1

    file_question_urls.close()
    file_data.close()
    return 0

# Gather user data and save into csv file
def crawlUser():
    if (DEBUG): print "In crawlUser..."
    
    unique_users = set(open("users.txt").readlines())
    bar = open('temp.txt', 'w').writelines(set(unique_users))
    
    file_users = open("temp.txt", mode='r')
    file_users_csv = open("users.csv", mode='w')
    total = 0
    
    current_line = file_users.readline()
    while(current_line):
        
        # Open browser to current_question_url
        chromedriver = "chromedriver"   # Needed?
        os.environ["webdriver.chrome.driver"] = chromedriver    # Needed?
        options = webdriver.ChromeOptions()

        ##chrome options to test
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')

        browser = webdriver.Chrome(chrome_options=options)
        browser.get(current_line)
        
        # Fetch page
        src_updated = browser.page_source
        src = ""
        while src != src_updated:
            time.sleep(.5)
            src = src_updated
            browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    
        # Find user id
        user_id = browser.current_url
        html_source = browser.page_source
        browser.quit()
        
        source_soup = BeautifulSoup(html_source)
        part = source_soup.find_all(attrs={"class":"link_label"})
        part_soup = BeautifulSoup(str(part))
        raw_info = part_soup.text.split(",")
        if (DEBUG): print raw_info
        
        for x in range(1, len(raw_info)):
            #if (DEBUG): print raw_info[x]
            key = raw_info[x].split(" ")[1]
            value = raw_info[x].split(" ")[2]
            if key == "Topics":
                num_topics = value
                if (DEBUG): print "num_topics:", num_topics
            elif key == "Blogs":
                num_blogs = value
                if (DEBUG): print "num_blogs:", num_blogs
            elif key == "Questions":
                num_questions = value
                if (DEBUG): print "num_questions:", num_questions
            elif key == "Answers":
                num_answers = value
                if (DEBUG): print "num_answers:", num_answers
            elif key == "Edits":
                value = value.split("]")[0]
                num_edits = value
                if (DEBUG): print "num_edit:", num_edits
        
        # Find followers
        followers_url = user_id.split('?')[0] + "/followers?share=1"
        browser = webdriver.Chrome()
        browser.get(followers_url)

        src_updated = browser.page_source
        src = ""
        while src != src_updated:
            time.sleep(.5)
            src = src_updated
            browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            src_updated = browser.page_source
        followers_html_source = browser.page_source
        browser.quit()

        followers_soup = BeautifulSoup(followers_html_source)
        followers_raw = followers_soup.find_all(attrs={"class":"user"})
        if (DEBUG): print "num of followers:", len(followers_raw)

        followers = ""
        count = 0
        for x in range(1, len(followers_raw)):
            followers_soup = BeautifulSoup(str(followers_raw[x]))
            for follower in followers_soup.find_all('a', href=True):
                count += 1
                if (followers):
                    followers += ", " + "http://www.quora.com" + follower['href'] + "?share=1"
                else:
                    followers += "http://www.quora.com" + follower['href'] + "?share=1"

        if (DEBUG): print "Followers count:", count
        followers = "{{{" + followers + "}}}"

        # Find following
        following_url = user_id.split('?')[0] + "/following?share=1"
        browser = webdriver.Chrome()
        browser.get(following_url)
        
        src_updated = browser.page_source
        src = ""
        while src != src_updated:
            time.sleep(.5)
            src = src_updated
            browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            src_updated = browser.page_source
        following_html_source = browser.page_source
        browser.quit()
        
        following_soup = BeautifulSoup(following_html_source)
        following_raw = following_soup.find_all(attrs={"class":"user"})
        if (DEBUG): print "num of following:", len(following_raw)
        
        following = ""
        count = 0
        for x in range(1, len(following_raw)):
            following_soup = BeautifulSoup(str(following_raw[x]))
            for each_following in following_soup.find_all('a', href=True):
                count += 1
                if (following):
                    following += ", " + "http://www.quora.com" + each_following['href'] + "?share=1"
                else:
                    following += "http://www.quora.com" + each_following['href'] + "?share=1"

        if (DEBUG): print "Following count:", count
        following = "{{{" + following + "}}}"

        s = user_id + ", " + str(num_topics) + ", " + str(num_blogs) + ", " + str(num_questions) + ", " + str(num_answers) + ", " + str(num_edits) + ", " + followers + ", " + following
        if (DEBUG): print s
        file_users_csv.write((s + '\n').encode('utf8'))
        current_line = file_users.readline()
        total += 1
    
    file_users.close()
    file_users_csv.close()
    print "Total users:{0}".format(str(total))
    return 0

# Reads a line of users.csv format and return the fields in separate variabes
def parseUsersFile(line):
    parts = line.split(',', 6)
    user_id = parts[0]
    number_of_upvotes = parts[1]
    number_of_blogs = parts[2]
    number_of_questions = parts[3]
    number_of_answers = parts[4]
    number_of_edits = parts[5]
    rest = parts[6]
    followers = rest.split('}}}', 2)[0].split('{{{')[1]
    following = rest.split('}}}', 2)[1].split('{{{')[1]
    return user_id, number_of_upvotes, number_of_blogs, number_of_questions, number_of_answers, number_of_edits, followers, following

# Reads a line of answers.csv format and return the fields in separate variabes
def parseAnswersFile(line):
    parts = line.split(',', 5)
    answer_id = parts[0]
    question_id = parts[1]
    user_id = parts[2]
    date = parts[3]
    number_of_upvotes = parts[4]
    rest = parts[5]
    users_who_upvoted = (rest.split('}}}')[0]).split('{{{')[1]
    topics = (rest.split('}}}',3)[1]).split('{{{')[1]
    if (DEBUG): print topics
    current_topics = (rest.split('}}}',3)[2]).split(',',2)[1].split(',',2)[0]
    if (DEBUG): print current_topics
    question_text = (rest.split('}}}',4)[2]).split('{{{')[1]
    if (DEBUG): print question_text
    answer_text = (rest.split('}}}',5)[3]).split('{{{')[1]
    if (DEBUG): print answer_text
    return answer_id, question_id, user_id, number_of_upvotes, users_who_upvoted, topics, current_topics, question_text, answer_text

def main():
    import sys
    filename = sys.argv[-1]
    # topics = crawlTopicHierarchy()
    # crawlTopicQuestions(topics)
    crawlQuestionData(filename)
    #crawlUser();
    return 0

if __name__ == "__main__": main()
