#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from datetime import datetime, timedelta
import errno
import time
import json
import pytz
import loggingging

import boto3
import numpy as np
import pandas as pd
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi

kargs = {k: v for k, v in [item.split('=') for item in sys.argv if '=' in item]}

timezone = pytz.timezone('America/Sao_Paulo')
today = datetime.now(timezone)
today_dmy_hms = today.strftime('%d-%m-%Y %H:%M:%S')

my_app_id = kargs['my_app_id']
my_app_secret = kargs['my_app_secret']
my_access_token = kargs['my_access_token']
my_accounts = kargs['my_accounts']

# Inicializa FacebookAdsApi passando o id do aplicativo, a chave secreta e o token de acesso
FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

# Nesse caso, a extração irá pegar o dia anterior (D -1)
date_delta = datetime.now() - timedelta(days=1)

date_delta_formated = date_delta.strftime('%Y-%m-%d')

filename = date_delta_formated + '_fb.csv'

# Usado para juntar as partes extraídas
list_dfs = []

# Uma ou mais contas que podem ser extraídos
my_accounts_json = json.loads(my_accounts)

try:
    for account_name, account_id in my_accounts_json.items():
        account = AdAccount("act_" + str(account_id))

        success = False
        max_retries = 5
        messages = []

        for x in range(1, max_retries):
            try:

                logging.info('Get insights ' + date_delta_formated)
                ads = account.get_insights(
                    params={'time_range': {'since': date_delta_formated, 'until': date_delta_formated}, 'level': 'ad'},
                    fields=[AdsInsights.Field.campaign_name,
                            AdsInsights.Field.adset_name,
                            AdsInsights.Field.ad_name,
                            AdsInsights.Field.impressions,
                            AdsInsights.Field.outbound_clicks,
                            AdsInsights.Field.spend])

                # logging.info("Tentativa " + str(x) + " OK!")
                break
                # messages = []
                # str_error = None
            except Exception as str_error:
                messages.append(str_error)
                logging.info("Aguardando 60s... ")
                time.sleep(60)
            else:
                if len(messages) > 0:
                    logging.error("Tentativas excedidas {} {}. Conta {} ".format(str(max_retries), str(messages[0]),
                                                                                 str(account_id)))

                    pass
        try:

            if ads:

                logging.info('Gerando lista')
                for ad in ads:
                    date = date_delta_formated
                    campaignname = ""
                    adsetname = ""
                    adname = ""
                    impressions = ""
                    outboundclicks = ""
                    spend = ""
                    accountname = account_name

                    if ('campaign_name' in ad):
                        campaignname = ad[AdsInsights.Field.campaign_name]
                    if ('adset_name' in ad):
                        adsetname = ad[AdsInsights.Field.adset_name]
                    if ('ad_name' in ad):
                        adname = ad[AdsInsights.Field.ad_name]
                    if ('impressions' in ad):
                        impressions = ad[AdsInsights.Field.impressions]
                    if ('outbound_clicks' in ad):
                        outboundclickslist = ad[AdsInsights.Field.outbound_clicks]
                        outboundclicksdict = outboundclickslist[0]
                        outboundclicks = outboundclicksdict.get('value')
                    if ('spend' in ad):
                        spend = ad[AdsInsights.Field.spend]

                    # Cria o dataframe e adiciona na lista
                    fbarray = np.array([[date, campaignname, adsetname, adname, impressions, outboundclicks, spend,
                                         accountname]])
                    df_part = pd.DataFrame(data=fbarray)
                    list_dfs.append(df_part)

        except Exception as e:
            logging.error(e)
            raise

    logging.info('Concatenando lista')
    df_final = pd.concat(list_dfs)

    # Adiciona o cabeçalho e gera o arquivo csv
    df_final.columns = ["date", "campaign_name", "adset_name", "ad_name", "impressions", "outbound_clicks",
                        "spend", "account_name"]
    df_final.to_csv(filename, index=False, encoding='utf-8', sep=';', header=True)

    s3_bucket = kargs['bucket-name']
    s3_folder = kargs['bucket-path']

    complete_s3_path = s3_folder + filename
    logging.info(complete_s3_path)

    s3 = boto3.resource('s3')

    logging.info('Escrevendo arquivo no S3')
    s3.meta.client.upload_file(filename,
                               s3_bucket,
                               complete_s3_path)

    logging.info(filename + ' extraido com sucesso em {}'.format(datetime.now()))

except Exception as e:
    logging.error(e)
    raise
