

#load pre-defined library
import classify_rumor

#configure system
import sys;
reload(sys);
sys.setdefaultencoding('utf8')

#data source
url = 'https://pheme.ushahidi.com/'

#authentication variables
user = 'chris@ihub.co.ke'
passwd = 'pheme2015'

#initialize class 
analysis = classify_rumor.classify(url,user,passwd)
spam = analysis.remove_spam()

#write results to csv
spam[0].to_csv('1.csv')
spam[1].to_csv('2.csv')
