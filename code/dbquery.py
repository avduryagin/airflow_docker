import cx_Oracle
import pandas as pd
import numpy as np
import os
from sklearn.decomposition import PCA
from sklearn.ensemble import IsolationForest
from tensorflow import keras



class idquery:
    def __init__(self,sql=None):
        if sql is None:
            self.sql="""select ID, NAME from TCH_WELLS 
WHERE NAME in {0} and FIELD_ID in (SELECT ID FROM TCH_FIELDS
WHERE NAME LIKE '{1}')"""
        else:
            if isinstance(sql,str):
                self.sql=sql
            else:
                self.sql=""""""
    def get_sql(self,*args,**kwargs):
        return self.sql.format(*args)


class session:
    def __init__(self,host='vps-oradb01.ois.ru',service_name='tmn',port='1521',user='OIS_ITECH',password='OIS_ITECH',**kwargs):
        self.host=host
        self.service_name=service_name
        self.port=port
        self.user=user
        self.log=''
        self.password=password
        self.dsn=cx_Oracle.makedsn(host=self.host,port=self.port,service_name=self.service_name,**kwargs)
        self.connection=None
        self.su_sql="""select S.ACURR,S.LOADFACTOR,S.PUMPINTAKEPRESSURE,S.ENGINETEMPERATURE,S.WORKINGFREQ,S.MEASDATE from TCH_WELLS w join TCH_ACTUALBORES_MV b
        on b.well_id = w.id
        join TM_SUMEASURES s
        on s.well_id = w.id
        and s.measdate > TO_DATE('{1}', 'DD.MM.YYYY')
        where w.id = '{0}'
        ORDER BY s.MEASDATE asc"""

        self.agzu_sql="""SELECT measdate, flowrate FROM TM_AGZUMEASURES WHERE WELL_ID IN ('{0}')
         and measdate > TO_DATE('{1}', 'DD.MM.YYYY')ORDER BY MEASDATE asc"""
        self.path = os.path.join(os.getcwd(), 'raw')
        if not os.path.isdir(self.path):
            os.mkdir(os.getcwd(),'raw')
    def open(self):
        self.connection = cx_Oracle.connect(user=self.user, password=self.password, dsn=self.dsn, encoding="UTF-8")

    def wrap(self,x):
        if len(x) > 1:
            return x
        elif len(x)==1:
            return """('""" + str(x[0]) + """')"""
        else:
            return """('"""  """')"""

    def get_id(self,diction=dict({}),**kwargs):
        IDs=[]
        if self.connection is None:
            self.open()
        query=idquery(**kwargs)

        for k in diction.keys():
            sql=query.get_sql(self.wrap(diction[k]),k)
            ID=pd.read_sql(sql,con=self.connection)
            if ID.shape[0]>0:
                ID['field']=k
                IDs.append(ID.values)
        if len(IDs)>0:
            values=np.vstack(IDs)
        else:
            values=IDs
        return pd.DataFrame(data=values,columns=['ID','well','field'])

    def get_agzu(self,wellid,date='01.06.2022'):
        if self.connection is None:
            self.open()
        sql=self.agzu_sql.format(str(wellid),date)
        agzu=pd.read_sql(sql,con=self.connection)
        return agzu

    def get_su(self, wellid, date='01.06.2022'):
        if self.connection is None:
            self.open()
        sql = self.su_sql.format(str(wellid), date)
        su = pd.read_sql(sql, con=self.connection)
        return su

    def get_data(self,d=dict({}),printlog=False,**kwargs):
        try:
            if not isinstance(d,dict):
                raise TypeError
        except TypeError:
            print('Expected a dictionary key="field",value=(name1,name2..namen')
            return None
        ID=self.get_id(d)
        agg=ID.groupby(by='field')
        msg = 'has been loaded measurements {0} for well {1} ID {4} of field {2}. shape={3}'
        for i in agg.groups:
            path = os.path.join(self.path, str(i))
            if not os.path.isdir(path):
                os.mkdir(path)
            group=agg.groups[i]
            for g in group:
                wellid=ID.loc[g,'ID']
                well=ID.loc[g,'well']
                agzu=self.get_agzu(wellid=str(wellid))
                agzumsg=msg.format('AGZU',well,i,agzu.shape[0],wellid)+'\n'
                self.log+=agzumsg
                if printlog:
                    print(agzumsg)
                agzu['MEASDATE']=pd.to_datetime(agzu['MEASDATE'])
                agzu.to_csv(os.path.join(path,'AGZU'+str(well)+".csv"),index_label=False)
                su = self.get_su(wellid=str(wellid))
                sumsg=msg.format('SU', well, i, su.shape[0],wellid) + '\n'
                self.log +=sumsg
                if printlog:
                    print(sumsg)
                su['MEASDATE'] = pd.to_datetime(su['MEASDATE'])
                su.to_csv(os.path.join(path, 'SU' + str(well) + ".csv"), index_label=False)

class pipeline_session(session):
    def __init__(self):
        super().__init__()
        self.su_sql = """select S.ACURR,S.LOADFACTOR,S.PUMPINTAKEPRESSURE,S.ENGINETEMPERATURE,S.WORKINGFREQ,S.MEASDATE from TCH_WELLS w join TCH_ACTUALBORES_MV b
        on b.well_id = w.id
        join TM_SUMEASURES s
        on s.well_id = w.id
        and s.measdate > TO_DATE('{1}', 'DD.MM.YYYY')
        and s.measdate < TO_DATE('{2}', 'DD.MM.YYYY')
        where w.id = '{0}'
        ORDER BY s.MEASDATE asc"""

    def get_su(self, wellid, sdate='01.06.2022',edate='02.06.2022'):
        if self.connection is None:
            self.open()
        sql = self.su_sql.format(str(wellid), sdate,edate)
        #return sql
        su = pd.read_sql(sql, con=self.connection)
        return su

class well_pipeline:
    def __init__(self,well,field):
        self.well=well
        self.field=field
        self.wellid=None
        self.session=None
        self.data=None
        self.values=None
        self.columns=['ACURR', 'LOADFACTOR', 'PUMPINTAKEPRESSURE', 'WORKINGFREQ',
       'ENGINETEMPERATURE']
        self.model=None

    def load_model(self,file=None,folder='models'):
        if file is None:
            return False
        path=os.path.join(os.getcwd(),folder)
        self.model=keras.models.load_model(os.path.join(path,file))
        return True

    def predict(self):
        if (self.model is None)|(self.values is None):
            return False
        predicted=self.model.predict(self.values[self.columns].values)
        negin=np.where(predicted<0.)[0]
        predicted[negin]=0
        self.values['predicted']=predicted
        self.values['avg']=np.nan
        grouped=self.values['predicted'].groupby(self.values.index.hour)
        for k in grouped.groups.keys():
            index=grouped.groups[k]
            self.values.loc[index,'avg']=self.values.loc[index,'predicted'].mean()


        return True




    def open(self,*args,**kwargs):
        self.session = pipeline_session(*args,**kwargs)

    def get_data(self,*args,sdate=np.datetime64('2022-06-01'),edate=np.datetime64('2022-06-02'),**kwargs):
        if self.session is None:
            self.open(*args,**kwargs)

        wellid=self.session.get_id(diction=dict({self.field:(self.well,)}))
        if wellid.shape[0]>0:
            self.wellid=wellid.iloc[0]['ID']
            data=self.session.get_su(wellid=self.wellid,sdate=pd.to_datetime(sdate).strftime('%d.%m.%Y') ,edate=pd.to_datetime(edate).strftime('%d.%m.%Y') )
            self.data=data

    def get_features(self):

        self.values=self.data.set_index('MEASDATE')
        #self.values.drop('MEASDATE',axis=1,inplace=True)
        self.values=self.values[self.columns]
        self.values.fillna(method='ffill',inplace=True)
        self.values.fillna(method='bfill',inplace=True)
        #self.data.sort_index(inplace=True)
        mask2 = ~((self.values['LOADFACTOR'] > 0.) & (self.values['LOADFACTOR'] <= 100.))
        mask3 = self.values['WORKINGFREQ'] < 0.
        mask4 = ~((self.values['ENGINETEMPERATURE'] > 0.) & (self.values['ENGINETEMPERATURE'] < 150.))
        mask5 = ~((self.values['PUMPINTAKEPRESSURE'] > 0.) & (self.values['PUMPINTAKEPRESSURE'] < 20.))
        mask = mask2 & mask3 & mask4 & mask5
        self.values.drop(mask.index[mask], inplace=True)
        n_empty=np.where(np.isnan(self.values.values))[0].shape[0]
        if n_empty>0:
            return False
        else:
            return True





def get_features(file,path=None,extention='.csv',prefix=dict({'AGZU':{'dtype':{'FLOWRATE': np.float64},'dates':['MEASDATE']},
                                                              'SU':{'dates':['MEASDATE'], 'dtype' :{'ACURR': np.float64, 'LOADFACTOR': np.float64,
                                                                               'PUMPINTAKEPRESSURE': np.float64,'ENGINETEMPERATURE': np.float64, 'WORKINGFREQ': np.float64}}})):

    if path is None:
        path=os.getcwd()
    files=dict({})
    for p in prefix.keys():
        f=str(p)+str(file)+str(extention)
        dates=prefix[p]['dates']
        dtype = prefix[p]['dtype']

        try:

            df = pd.read_csv(os.path.join(path, f), parse_dates=dates, infer_datetime_format=True,
                               dayfirst=True, engine='c', dtype=dtype)
            if p=='AGZU':
                mask = df['FLOWRATE'] < 0.
                df.drop(mask.index[mask], inplace=True)
            if p=='SU':
                df = df.fillna(method='ffill')
                df = df.fillna(method='bfill')
                df.set_index('MEASDATE', inplace=True)
                df.sort_index(inplace=True)
                mask2 = ~((df['LOADFACTOR'] > 0.) & (df['LOADFACTOR'] <= 100.))
                mask3 = df['WORKINGFREQ'] < 0.
                mask4 = ~((df['ENGINETEMPERATURE'] > 0.) & (df['ENGINETEMPERATURE'] < 150.))
                mask5 = ~((df['PUMPINTAKEPRESSURE'] > 0.) & (df['PUMPINTAKEPRESSURE'] < 20.))
                mask = mask2 & mask3 & mask4 & mask5
                df.drop(mask.index[mask], inplace=True)

            files.update({p:df})
        except FileNotFoundError:
            pass

    return files

def get_variance_ratios(file,path,return_frame=False,**kwargs):

    def get_dict(*args,**kwargs):
        features=get_features(file, path, **kwargs)
        if len(features.keys())==0:
            return dict({})
        RES=dict({file:{}})
        RES_=RES[file]
        for k in features.keys():
            RES_.update({k:{}})
            res=RES_[k]

            if k=='SU':
                columns=features[k].columns
                values = features[k].values
                variance = values.std(axis=0)
                means = values.mean(axis=0)
                pca=PCA(n_components=values.shape[1])
                pca.fit(values)
                explained_varience=pca.explained_variance_ratio_
                res.update({'explained_ratio':explained_varience})
                i=0
                for c in columns:
                    res.update({c:{'var':variance[i],'mean':means[i]}})
                    i+=1
            elif k=='AGZU':
                values = features[k]['FLOWRATE'].values
                mask=~np.isnan(values)
                nnan=np.where(mask)[0].shape[0]
                count=values.shape[0]
                ratio=nnan/count
                zeros=np.where(values==0)[0].shape[0]
                zeros_ratio=zeros/nnan
                ent_zeros_ratio = zeros / count
                var=values.std()
                mean=values.mean()
                res.update({'var':var,'mean':mean,'shape':count,'nempty':nnan,'nempty_ratio':ratio,'zeros':zeros,'zeros_ratio':zeros_ratio,'entire_0ratio:':ent_zeros_ratio})
        return RES

    def get_frame(*args,**kwargs):
        features=get_features(file, path, **kwargs)
        if len(features.keys())==0:
            return np.array([])
        columns_=list(features["SU"].columns)
        #print(columns_)
        columns_.append('FLOATRATE')
        index=['explained_ratio','var', 'mean', 'shape', 'nempty', 'nempty_ratio', 'zeros', 'zeros_ratio', 'entire_zeratio:']
        array=np.empty(shape=(len(index),len(columns_)))
        array.fill(np.nan)
        vempty=np.empty(features["SU"].columns.shape[0])
        vempty.fill(np.nan)

        for k in features.keys():
            if k=='SU':

                values = features[k].values
                variance = values.std(axis=0)
                means = values.mean(axis=0)
                pca=PCA(n_components=values.shape[1])
                try:
                    pca.fit(values)
                    explained_varience=pca.explained_variance_ratio_
                except ValueError:
                    explained_varience=vempty
                #print(array[0,:-1].shape,explained_varience.shape,array.shape)
                array[0,:-1]=explained_varience
                array[1, :-1]=variance
                array[2, :-1] =means
                array[3,:-1].fill(values.shape[0])
                i=0
                while i<values.shape[1]:
                    col=values[:,i]
                    mask = ~np.isnan(col)
                    nnan = np.where(mask)[0].shape[0]
                    array[4,i]=nnan
                    array[5, i] = nnan/col.shape[0]
                    zeros = np.where(col == 0)[0].shape[0]
                    array[6, i] = zeros
                    if nnan>0:
                        zeros_ratio = zeros / nnan
                    else:
                        zeros_ratio = 0
                    array[7, i] = zeros_ratio
                    ent_zeros_ratio = zeros / col.shape[0]
                    array[8, i]=ent_zeros_ratio
                    i+=1



            elif k=='AGZU':
                values = features[k]['FLOWRATE'].values
                mask=~np.isnan(values)
                nnan=np.where(mask)[0].shape[0]
                count=values.shape[0]
                ratio=nnan/count
                zeros=np.where(values==0)[0].shape[0]
                if nnan>0:
                    zeros_ratio=zeros/nnan
                else:
                    zeros_ratio=0
                ent_zeros_ratio = zeros / count
                var=values.std()
                mean=values.mean()
                array[0,-1]=var
                array[1, -1] = var
                array[2, -1] = mean
                array[3, -1] = count
                array[4, -1] = nnan
                array[5, -1] = ratio
                array[6, -1] = zeros
                array[7, -1] = zeros_ratio
                array[8, -1] = ent_zeros_ratio

        return pd.DataFrame(data=array,columns=columns_,index=index)



    if return_frame:
        return get_frame(file,path,**kwargs)
    else:
        return get_dict(file,path,**kwargs)

class isolation_forest:
    def __init__(self,n_estimators=50, max_samples='auto', contamination=0.003,max_features=1.0,n_jobs=-1,**kwargs):
        self.model=IsolationForest(n_estimators=n_estimators, max_samples=max_samples, contamination=contamination,max_features=max_features,n_jobs=n_jobs,**kwargs)
        self.scores=np.array([])
        self.labels = np.array([])
    def fit_predict(self,x=np.array([])):
        self.model.fit(x)
        self.labels=self.model.predict(x)
        self.scores=self.model.decision_function(x)



















