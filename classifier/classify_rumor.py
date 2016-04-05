
#load required libraries
import numpy as np
import ushahidiv2 as ush
import pandas as pd

class classify(object):

    #initialize class
    def __init__(self,url,user,passwd):
        self.col= ['incident_id', 'message_from', 'message_to', 'message_date','message_text','locationlatitude','message_detail','parent_id','reporter_id','message_level','service_message_id','message_type','message_id','locationlongitude']
        self.ushahidi_reports = ush.get_all_reports(url)['incidentdescription']
        self.messages =  pd.DataFrame(ush.get_messages(url,user,passwd), columns=self.col)['message_text']

    #define levenstein distance   
    def levenstein(self,s,t):
        s = ' ' + s
        t = ' ' + t
        d = {}
        S = len(s)
        T = len(t)
        for i in range(S):
            d[i, 0] = i
        for j in range (T):
            d[0, j] = j
        for j in range(1,T):
            for i in range(1,S):
                if s[i] == t[j]:
                    d[i, j] = d[i-1, j-1]
                else:
                    d[i, j] = min(d[i-1, j] + 1, d[i, j-1] + 1, d[i-1, j-1] + 1)
        return d[S-1, T-1]
   
    #calculate report mean/centroid value
    def centroid(self):
        distances = []
        for a,i in enumerate(self.ushahidi_reports) :
            for b,j in enumerate(self.ushahidi_reports):
                if (a!=b and a-b>0):
                    dist = self.levenstein(i,j)
                    distances.append(dist)
        n = np.mean(distances)
        return n

    #remove spam
    def remove_spam(self):
        #container for categories
        accept = list()
        decline = list()

        #cluster center
        center = self.centroid()
        
        #classfity messages
        for k,m in enumerate(self.messages):
            for l,u in enumerate(self.ushahidi_reports):
                m_dist = self.levenstein(m,u)
                if(m_dist < center):
                    accept.append(k)
                else:
                    decline.append(k)

        spam = self.messages[decline]
        non_spam = self.messages[accept]

        return non_spam,spam
