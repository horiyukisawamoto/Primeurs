import pandas as pd
from bs4 import BeautifulSoup
import requests
from urllib.request import Request, urlopen
import numpy as np
import json
import time
import re
import unidecode
import xlsxwriter
import pandas.io.formats.excel
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error,mean_squared_error

class Primeurs:

    def __init__(self,api_format,token):

        self.api_format = api_format
        self.token = token

    def get_gws(self):

        headers = {
          'Accept': self.api_format,
          'Authorization': 'Token ' + self.token
        }
        request = Request('https://api.globalwinescore.com/globalwinescores/latest/?vintage=2018&limit=1500', headers=headers)

        response_body = urlopen(request).read()

        data = json.loads(response_body)['results']

        conf_convert = {'C':1,'C+':2,'B':3,'B+':4,'A':5,'A+':6}
        vin = []
        self.vin_dict = {}
        vin_detail = []
        self.vin_detail_dict = {}
        appellation = []
        self.appellation_dict = {}
        color = []
        score = []
        confidence_index = []
        journalist_count = []

        for elem in data:
            vin.append(re.sub("[^a-zA-Z]+", "",elem.get('wine').split(',')[0].lower().replace('chateau','')))
            appellation.append(re.sub("[^a-zA-Z]+", "",elem.get('appellation').lower()))
            color.append(elem.get('color'))
            score.append(elem.get('score'))
            confidence_index.append(conf_convert.get(elem.get('confidence_index')))
            journalist_count.append(elem.get('journalist_count'))
            vin_detail.append(re.sub("[^a-zA-Z]+", "",elem.get('wine').split(',')[1][1:].lower().replace('chateau','')))

            self.vin_dict[re.sub("[^a-zA-Z]+", "",elem.get('wine').split(',')[0].lower().replace('chateau',''))] = elem.get('wine').split(',')[0]
            self.vin_detail_dict[re.sub("[^a-zA-Z]+", "",elem.get('wine').split(',')[1][1:].lower().replace('chateau',''))] = elem.get('wine').split(',')[1][1:]
            self.appellation_dict[re.sub("[^a-zA-Z]+", "",elem.get('appellation').lower())] = elem.get('appellation')

        df_gws = pd.DataFrame(zip(vin,vin_detail,appellation,color,score,confidence_index,journalist_count), columns=['Vin','VinDetail','Appellation','Color','Score','Confidence Index','Journalist Count'])

        df_gws['VinDetail'] = np.where(np.logical_or(df_gws['VinDetail']==df_gws['Appellation'],df_gws['VinDetail']=='blanc'),"-",df_gws['VinDetail'])

        df_gws['Appellation'] = df_gws['Appellation'].replace('bordeauxblanc','bordeaux')
        df_gws['Appellation'] = df_gws['Appellation'].replace('moulisenmedoc','moulis')

        return df_gws

    def scrape_chatprim(self):

        wine_name_list = []
        wine_appellation_list = []
        wine_color_list = []
        wine_price_list = []
        wine_url = []
        wine_status_list = []

        i = 1
        while i < 20:

            response = requests.get('https://www.chateauprimeur.com/catalogue/tous/2019?url=Grand-vins-Bordeaux-primeur-2019&page={}'.format(i))

            soup = BeautifulSoup(response.content,'lxml')

            wine_name_soup = soup.find_all('a',class_='produit_nom')
            wine_appellation_soup = soup.find_all('a',class_='produit_appellation')
            wine_price_soup = soup.find_all('span',class_='prix')
            wine_url_soup = soup.find_all('a',class_='produit_nom')
            wine_color_soup = soup.find_all('a',class_='produit_appellation')
            wine_status_soup_avenir = soup.find_all('a',class_='btn boxshadows avenir')
            wine_status_soup_epuise = soup.find_all('a',class_='btn boxshadows epuise')

            for wine in wine_name_soup:
                wine_name_list.append(re.sub("[^a-zA-Z]+", "",unidecode.unidecode(wine.find('strong').get_text().lower().replace("château",""))))

            for wine in wine_appellation_soup:
                wine_appellation_list.append(re.sub("[^a-zA-Z]+", "",unidecode.unidecode(wine.get_text().lower())))

            for wine in wine_price_soup:
                if wine.find('strong') is not None:
                    wine_price_list.append(float(wine.find('strong').get_text().replace(',','.').replace('€','')))

            for wine in wine_status_soup_avenir:
                    wine_price_list.append(wine.get_text())

            for wine in wine_status_soup_epuise:
                wine_price_list.append(wine.get_text())

            for wine in wine_url_soup:
                    wine_url.append('https://www.chateauprimeur.com' + wine['href'])

            for wine in wine_color_soup:
                if 'blancs-secs' in wine['href'] or 'liquoreux' in wine['href']:
                    wine_color_list.append('White')
                else:
                    wine_color_list.append('Red')

            i += 1

        df_cp = pd.DataFrame(zip(wine_name_list,wine_appellation_list,wine_color_list,wine_price_list,wine_url), columns=['Vin','Appellation','Color','Prix € HT','URL'])

        df_cp['Vin'] = df_cp['Vin'].apply(lambda x: x.replace('blanc','') if x not in ['chevalblanc','latourblancheblanc','domainesimonblanchard','jacquesblanc','moulindeblanchon'] else x)

        df_cp[(df_cp['Prix € HT']!='A venir') & (df_cp['Prix € HT']!='Epuisé') & (df_cp['Prix € HT']!='Non mis en marché')].to_csv('chatprim_price_list.csv',mode='a',index=False,header=False)

        unique_price_list = pd.read_csv('chatprim_price_list.csv')
        unique_price_list.drop_duplicates(inplace=True,subset=None)
        unique_price_list.to_csv('chatprim_price_list.csv', index=False)

        return df_cp

    def merge_df(self,df1,df2):

        self.get_gws()

        df_first_merge = pd.merge(df1,df2[['Prix € HT','Vin','Appellation','Color', 'URL']],how='inner', left_on=['VinDetail','Appellation','Color'], right_on=['Vin','Appellation','Color'])
        df_first_merge.drop('Vin_y',inplace=True,axis=1)
        df_first_merge.rename(columns={'Vin_x':'Vin'},inplace=True)

        df_sec_merge = pd.merge(df1,df2[['Prix € HT','Vin','Appellation','Color','URL']],how='inner', left_on=['Vin','Appellation','Color'], right_on=['Vin','Appellation','Color'])
        df_sec_merge = df_sec_merge[df_sec_merge['VinDetail']=='-']

        df_final = pd.concat([df_first_merge,df_sec_merge], axis=0)
        df_final.drop_duplicates(inplace=True)

        df_chatprim_price_list = pd.read_csv('chatprim_price_list.csv')

        df_final_epuise = df_final[df_final['Prix € HT']=='Epuisé']

        merged = pd.merge(df_final_epuise,df_chatprim_price_list,how='inner', on=['URL','Color','Appellation'])
        merged.drop(['Prix € HT_x', 'Vin_y'],inplace=True,axis=1)
        merged.rename(columns={'Vin_x':'Vin', 'Prix € HT_y':'Prix € HT'},inplace=True)
        merged['URL'] = 'Epuisé'

        df_final = df_final[df_final['Prix € HT']!='Epuisé']

        df_final = pd.concat([df_final,merged], axis=0)

        df_final['Vin'] = df_final['Vin'].apply(lambda x: self.vin_dict.get(x.replace('blanc','')) if x not in ['chevalblanc','latourblancheblanc','domainesimonblanchard','jacquesblanc','moulindeblanchon'] else self.vin_dict.get(x))
        df_final['VinDetail'] = df_final['VinDetail'].apply(lambda x: self.vin_detail_dict.get(x))
        df_final['Appellation'] = df_final['Appellation'].apply(lambda x: self.appellation_dict.get(x))

        df_dispo_epuise = df_final[(df_final['Prix € HT']!='A venir') & (df_final['Prix € HT']!='Epuisé')]
        df_dispo_epuise = df_dispo_epuise[df_dispo_epuise['Prix € HT']<=400]

        df_a_venir = df_final[(df_final['Prix € HT']=='A venir') | (df_final['Prix € HT']=='Epuisé')]

        return(df_dispo_epuise,df_a_venir)

    def reg_model(self,df):

        random_state = 0

        self.Note = np.array(df['Score'].astype('float'))
        self.Price = np.array(df['Prix € HT'].astype('float'))

        X_train, X_test, y_train, y_test = train_test_split(self.Note,self.Price,test_size=0.3,random_state=random_state)
        X_train2, X_test2, y_train2, y_test2 = train_test_split(self.Note.reshape(-1,1),self.Price,test_size=0.3,random_state=random_state)
        X_train3, X_test3, y_train3, y_test3 = train_test_split(self.Note.reshape(-1,1),self.Price,test_size=0.3,random_state=random_state)

        Exp = np.polyfit(X_train,np.log(y_train),1)
        ExpW = np.polyfit(X_train,np.log(y_train), 1, w=np.sqrt(y_train))

        Poly = PolynomialFeatures(3)
        model = LinearRegression()
        X_train2 = Poly.fit_transform(X_train2)
        X_test2 = Poly.fit_transform(X_test2)
        model.fit(X_train2,y_train2)

        RFR = RandomForestRegressor(n_estimators = 10, random_state=random_state)
        RFR.fit(X_train3, y_train3)

        y_pred_Exp = np.exp(Exp[1]) * np.exp(Exp[0] * X_test)
        y_pred_ExpW = np.exp(ExpW[1]) * np.exp(ExpW[0] * X_test)
        y_pred_Poly = model.predict(X_test2)
        y_pred_RFR = RFR.predict(X_test3)

        rmse_list = ['Exp','ExpW','Poly','RFR']
        self.rmse_value_list = [np.sqrt(mean_squared_error(y_test, y_pred_Exp)), np.sqrt(mean_squared_error(y_test, y_pred_ExpW)),np.sqrt(mean_squared_error(y_test, y_pred_Poly)),np.sqrt(mean_squared_error(y_test, y_pred_RFR))]
        self.reg_list = [ np.exp(Exp[1]) * np.exp(Exp[0] * self.Note),np.exp(ExpW[1]) * np.exp(ExpW[0] * self.Note),model.predict(Poly.fit_transform(self.Note.reshape(-1,1))),RFR.predict(self.Note.reshape(-1,1))]

        rmse_value_dict = dict(zip(self.rmse_value_list,rmse_list))
        reversed_rmse_value_dict = dict(zip(rmse_list,self.rmse_value_list))
        rmse_reg_dict = dict(zip(self.rmse_value_list,self.reg_list))

        best_algo = rmse_value_dict.get(min(rmse_reg_dict))

        df['ModelPrice' + best_algo] = rmse_reg_dict.get(min(rmse_reg_dict))

        df['Gain/Loss €'] = df['ModelPrice' + best_algo] - df['Prix € HT']
        df['Gain/Loss %'] = df['Gain/Loss €']/df['Prix € HT']

        df = df[[ col for col in df.columns if col != 'URL' ] + ['URL']]

        df = df.sort_values(by=['Gain/Loss %'], ascending=False)

        return df

    def xls_export(self,df1,df2):

        plt.style.use('ggplot')

        TodaysDate = time.strftime("%d-%m-%Y")

        with pd.ExcelWriter('Primeurs_under_400 - ' + TodaysDate + '.xlsx') as writer:
            pandas.io.formats.excel.ExcelFormatter.header_style = None

            sheets_in_writer = ['Primeurs_Dispo_ou_Epuise','Primeurs_A_Venir','ModelChart']
            df_for_writer = [df1,df2]

            for i,j in zip(df_for_writer,sheets_in_writer):
                i.to_excel(writer,j,index=False, startrow=1, header=False)

            workbook  = writer.book

            palette ={"White":"gold","Red":"firebrick"}
            sns.set_style("dark")
            sns.set(rc={'figure.figsize':(11.7,8.27)})

            sns.scatterplot(df1['Score'],df1['Prix € HT'],hue=df1['Color'], palette=palette).get_figure()
            plt.xlabel('Score')
            plt.ylabel('Price')
            sns.lineplot(self.Note, self.reg_list[2],color = 'cyan', label ='Poly, RMSE:' + str(int(self.rmse_value_list[2]))).get_figure()
            sns.lineplot(self.Note, self.reg_list[0],color = 'chocolate', label ='Exp, RMSE:' + str(int(self.rmse_value_list[0]))).get_figure()
            sns.lineplot(self.Note, self.reg_list[1],color = 'seagreen', label ='ExpW, RMSE:' + str(int(self.rmse_value_list[1]))).get_figure()
            sns.lineplot(self.Note, self.reg_list[3],color = 'navy', label ='RFR, RMSE:' + str(int(self.rmse_value_list[3]))).get_figure()

            plt.savefig('output.png',dpi=400)

            workbook.add_worksheet('ModelChart').insert_image('A1','output.png')

            col_format = workbook.add_format({'bold': False, 'font_color': 'black','font_size': 11,'valign': 'vcenter'})
            header_format = workbook.add_format({'bold': True,'text_wrap': True,'valign': 'vcenter','fg_color': '#FFBDBD','border': 0, 'font_size':11})

            for i,j in zip(df_for_writer,sheets_in_writer):
                writer.sheets[j].set_zoom(75)
                for col_num, value in enumerate(i.columns.values):
                    writer.sheets[j].write(0, col_num, value, header_format)
                for x, col in enumerate(i.columns):
                    column_len = max(i[col].astype(str).str.len().max(), len(col))
                    writer.sheets[j].set_column(x, x, column_len)

            workbook.get_worksheet_by_name("ModelChart").set_zoom(70)

            curr = workbook.add_format({'num_format': '"€"#,##0.00'})
            percentag = workbook.add_format({'num_format': '0%'})
            formatgreen  = workbook.add_format({'font_color':'#196F3D'})
            formatred = workbook.add_format({'font_color':'#FF0000'})

            writer.sheets['Primeurs_Dispo_ou_Epuise'].set_column('H:J', 18, curr)
            writer.sheets['Primeurs_Dispo_ou_Epuise'].set_column('K:K', 18, percentag)
            writer.sheets['Primeurs_Dispo_ou_Epuise'].conditional_format('J2:K300',{'type':'cell','criteria': '>','value':0.00,'format':formatgreen})
            writer.sheets['Primeurs_Dispo_ou_Epuise'].conditional_format('J2:K300',{'type':'cell','criteria': '<','value':0.00,'format':formatred})

        return (df1.to_excel(writer,sheet_name='Primeurs_Dispo_ou_Epuise', index=False,startrow=1, header=False),df2.to_excel(writer,sheet_name='Primeurs_A_Venir', index=False,startrow=1, header=False))

if __name__ == '__main__':

    api_format = 'application/json'
    token = 'cbe0b6bd69ff3ca987d312f178bc3ef0dfe4724f'
    p = Primeurs(api_format,token)
    primeurs_dispo_ndispo = p.merge_df(p.get_gws(),p.scrape_chatprim())
    primeurs_model = p.reg_model(primeurs_dispo_ndispo[0])
    p.xls_export(primeurs_model,primeurs_dispo_ndispo[1])
