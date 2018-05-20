# In this script we are merging all the files together. test Github	
	

	import pandas as pd
	import numpy as np
	import multiprocessing
	import boto3
	import os
	import datetime 
	import configparser
	import tempfile
	import concurrent.futures as futures
	pd.options.display.max_columns = 100000
	

	import logging
	logger = logging.getLogger('automated_testing')
	logger.setLevel(logging.DEBUG)
	fh=logging.FileHandler('audits.log', mode='w')
	fh.setLevel(logging.DEBUG)
	formatter = logging.Formatter('[%(asctime)s] %(levelname)8s :\n %(message)s ' +
	                                  '(%(filename)s:%(lineno)s)',datefmt='%Y-%m-%d %H:%M:%S')
	fh.setFormatter(formatter)
	logger.addHandler(fh)
	

	CREDENTIALS_PATH = '~/.aws/credentials'
	

	local_path = './shakenbake/data'
	s3_src_bucket = 'card-prtnr-npi'
	s3_src_prefix = 'retaildata/rtlone/'
	s3_dest_bucket = 'card-prtnr-npi'
	s3_dest_prefix = 'retaildata/rtlone'
	

	# read and assign aws token credentials
	full_credentials_path = os.path.expanduser(CREDENTIALS_PATH)
	parser = configparser.ConfigParser()
	parser.read(full_credentials_path)
	has_ptr_da_sa = parser.has_section('GR_GG_COF_AWS_PartnershipsDA_Prod_SharedAnalyst')
	src_profile = 'GR_GG_COF_AWS_PartnershipsDA_Prod_SharedAnalyst'
	src_access_key = parser.get(src_profile, 'aws_access_key_id')
	src_secret_key = parser.get(src_profile, 'aws_secret_access_key')
	src_security_token = parser.get(src_profile, 'aws_security_token')
	

	

	# create boto s3 connection object for source connection
	src_session = boto3.Session(
	    aws_access_key_id=src_access_key,
	    aws_secret_access_key=src_secret_key,
	    aws_session_token=src_security_token,
	)
	

	################################### For Versioning Date ########################################################
	# create boto s3 connection object for version control date 
	parser.read(full_credentials_path)
	has_sts_prod = parser.has_section('GR_GG_COF_AWS_STS_Prod_ProdSupport')
	date_profile = 'GR_GG_COF_AWS_STS_Prod_ProdSupport'
	date_access_key = parser.get(date_profile, 'aws_access_key_id')
	date_secret_key = parser.get(date_profile, 'aws_secret_access_key')
	date_security_token = parser.get(date_profile, 'aws_security_token')
	

	# create boto s3 connection object for source connection
	date_session = boto3.Session(
	    aws_access_key_id=date_access_key,
	    aws_secret_access_key=date_secret_key,
	    aws_session_token=date_security_token,
	)
	now = datetime.datetime.now() - datetime.timedelta(days=1)
	timestamp = str(now.year)
	prefix = 'us/whirl_cms/ambs_ambivolatile/'+ timestamp
	

	s3_date_bucket = 'prtnrshp-prod-raw'
	s3 = date_session.resource('s3')
	KEY = [filename.key for filename in s3.Bucket(s3_date_bucket).objects.filter(Prefix = prefix) ]
	KEY = KEY[-1]
	path = os.path.dirname(KEY)
	path = os.path.basename(path)
	print(" This is the path {}".format(path))
	print(type(path))
	###################################################################################################################
	# Function to convert julian dates to gregorian
	def jul_to_gre(l):
	    l=l.astype(str)
	    julian = l.str[4:].str.extract("([1-9][0-9]?[0-9]?)", expand =False)
	    dates = l.str[:4] + "-" + julian
	    dates = pd.to_datetime(dates, format ='%Y-%j', errors = 'coerce')
	    return dates
	

	# Function to download the files
	def filedownload(key, session):
	    with tempfile.TemporaryDirectory() as tempdir:
	        print("downloading {}/{}".format(s3_src_bucket, key))
	        session.client('s3').download_file(Bucket=s3_src_bucket, Key=key,
	                                                   Filename=os.path.join(tempdir, os.path.basename(key)))
	        df = pd.read_csv(os.path.join(tempdir, os.path.basename(key)), sep="\x01")
	        float_cols = df.columns[df.dtypes== np.float64 ]
	        print(float_cols)
	        #df[float_cols] = df[float_cols].astype(np.int64)
	    return df
	

	

	

	def risk_pass(df):
	    df['dmcut'] = 'riskfail'
	    df['emcut'] = 'riskfail'
	    df['emvalcut'] = 'valriskfail'
	    df['dmvalcut'] = 'valriskfail'
	

	    riskcut = []
	    dmcut = []
	    LMT_INC_DEC_IND = []
	    for i, row in df.iterrows():
	        # Function to find the difference in months
	        def diff_month(d1,d2):return (d1.year -d2.year)*12+d1.month-d2.month
	        df.at[i, 'MTH_SNC_LMTCHG'] = diff_month(datetime.datetime.today() , row['MAX_DATE_CRLIM_DATE_OPENED'])
	

	        # Identying SAKS Cobrand Logo's
	        if (row['ORG'] in [601]) & (row['LOGO'] in [300,700,304,314,394]):
	            df.at[i, 'SAKS_MC'] ='Y'
	        elif(row['ORG'] in [601]):
	            df.at[i,'SAKS_MC'] ='N'
	        else:
	            df.at[i,'SAKS_MC'] = 'N/A'
	            
	        if (row['LAST_CRLIM'] == row['CRLIM']) | (row['LAST_CRLIM'] == 0 & ((row['DATE_CRLIM'] is np.nan) | (row['DATE_CRLIM'] <= row['DATE_OPENED']))):
	            df.at[i, 'LMT_INC_DEC_IND'] = "N"
	        elif (row['LAST_CRLIM'] > row['CRLIM']):
	            df.at[i, 'LMT_INC_DEC_IND'] = "D"
	        elif (row['LAST_CRLIM'] < row['CRLIM']):
	            df.at[i, 'LMT_INC_DEC_IND'] = "I"
	        else:
	            df.at[i,'LMT_INC_DEC_IND'] = " "
	        ####################################################################    
	        # Value Flags
	        if row['DW_ATH_CHAIN_NBR'] in [135012247, 135585976, 137997710, 141281244, 456405481,458375230, 129240562, 129385255]:
	            df.at[i,'valuesirk'] = 'cloudonly_accounts'
	            # Derogatory Or Permanent Internal Status Codes
	        elif row['INT_STATUS'] in ['B', 'C', 'F', 'H', 'P', 'R', 'T', 'X', 'Z', '8', '9']:
	            df.at[i,'valuesirk'] = 'valrisk01'
	            # Derogatory Or Permanent Block Codes
	        elif (row['BLOCK_CODE_1'] in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'I', 'J', 'K', 'L', 'M', 'N','T', 'O', 'P', 'R',
	                                     'U', 'W', 'Y', 'Z']) | (row['BLOCK_CODE_2'] in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'I','T', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'R',
	                                     'U', 'W', 'Y', 'Z']):
	            df.at[i,'valuesirk'] = 'valrisk02'
	            # Hardship Indicator Not Equal To Blank
	        elif pd.notnull(row['pgm_coll_cd']):
	            df.at[i,'valuesirk'] = 'valrisk03'
	            # Accounts that have settlement arrangements in place
	        elif row['coll_stat_u28_cd'] in ['PS', 'PF', 'SI']:
	            df.at[i,'valuesirk'] = 'valrisk04'
	            # Foreign Countries - NOTE this includes US Territories (No Privacy Rule states we have to exclude those)
	        elif row['STATE_0'] not in ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA',
	                                        'ID', 'IL', 'IN', 'KS', 'KY',
	                                        'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH',
	                                        'NJ', 'NM', 'NV', 'NY', 'OH',
	                                        'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI',
	                                        'WV', 'WY', 'AS', 'GU', 'MP',
	                                        'PR', 'VI', 'UM']:
	              df.at[i,'valuesirk'] = 'valrisk05'
	           # Delinquent Accounts defined as one day or more delinquent
	        elif row['dlq_day_cnt'] > 60:
	            df.at[i,'valuesirk'] = 'valrisk06'
	        else:
	            df.at[i,'valuesirk'] = 'valrisk00'
	            
	            #  Valpop Direct Mail Flags
	            if (row['RTN_MAIL_CTR'] > 0) | (row['BLOCK_CODE_1'] in ['J']) | (row['BLOCK_CODE_2'] in ['J']):
	                df.at[i, 'dmvalcut'] = 'dmval01'
	            else:
	                df.at[i,'dmvalcut'] = 'dmval00'
	                
	            # Valpop Email Flags
	            # Do Not Solicit by optouts
	            if row['email_cd'] in ['X']:
	                df.at[i, 'emvalcut'] = 'emvalcut01'
	            # Non Whitelist Purged emails
	            elif (row['EMAIL_0'] == 'none@capitalone-no-email.com') | (row['EMAIL_0'] == 'none@capitalone.com'):
	                df.at[i, 'emvalcut'] = 'emvalcut02'
	            # Enterprise Whitelist scrub
	            elif (pd.isnull(row['whtlsemail'])):
	                df.at[i, 'emvalcut'] = 'emvalcut03'
	            
	            # Removal of Canadian email addresses as they have different privacy requirements
	            elif pd.isnull(row['EMAIL_0']):
	                df.at[i, 'emvalcut'] = 'emvalcut04'
	            elif row['EMAIL_0'][-3:] == '.ca':
	                df.at[i, 'emvalcut'] = 'emvalcut05'
	            # Wireless Domains - exclude any email address which matches the wireless domain at http://transition.fcc.gov/Bureaus/CGB/DomainNames/DomainNames.txt
	            elif row['DMN'] == row['domains']:
	                df.at[i, 'emvalcut'] = 'emvalcut06'
	            else:
	                df.at[i,'emvalcut'] = 'emvalcut00'
	          
	            
	            ########################################################################
	        
	            # Risk Flags
	            # Cloud Only accounts
	        if row['DW_ATH_CHAIN_NBR'] in [135012247, 135585976, 137997710, 141281244, 456405481,458375230, 129240562, 129385255]:
	            df.at[i,'riskcut'] = 'cloudonly_accounts'
	            # Derogatory Or Permanent Internal Status Codes
	        elif row['INT_STATUS'] in ['B', 'C', 'F', 'H', 'P', 'R', 'T', 'X', 'Z', '8', '9']:
	            df.at[i,'riskcut'] = 'risk01'
	            # Derogatory Or Permanent Block Codes
	        elif (row['BLOCK_CODE_1'] in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'I', 'J', 'K', 'L', 'M', 'N','T', 'O', 'P', 'R',
	                                     'U', 'W', 'Y', 'Z']) | (row['BLOCK_CODE_2'] in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'I','T', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'R',
	                                     'U', 'W', 'Y', 'Z']):
	            df.at[i,'riskcut'] = 'risk02'
	            # Hardship Indicator Not Equal To Blank
	        elif pd.notnull(row['pgm_coll_cd']):
	            df.at[i,'riskcut'] = 'risk03'
	            # Reage In Last Twelve Months
	        elif row['DATE_LAST_REAGE'] > (datetime.datetime.today() - datetime.timedelta(days=365)):
	            df.at[i,'riskcut'] = 'risk04'
	            # Cease And Desist Accounts
	        elif row['cease_desist_cd'] in ['AT', 'CD', 'CN', 'CV', 'PA', 'QU']:
	            df.at[i,'riskcut'] = 'risk05'
	        # Accounts that have settlement arrangements in place
	        elif row['coll_stat_u28_cd'] in ['PS', 'PF', 'SI']:
	            df.at[i,'riskcut'] = 'risk06'
	            # Insurance Claims
	            # elif row['CLAIM_STAT_CDE'] is not np.nan:
	            #rtl_one.at[i,'riskcut'] = 'risk07'
	            # Available Credit Less Than 0
	        elif row['OPEN_TO_BUY'] <= 0:
	            df.at[i,'riskcut'] = 'risk08'
	            # Delinquent Accounts defined as one day or more delinquent
	        elif row['dlq_day_cnt'] > 0:
	            df.at[i,'riskcut'] = 'risk09'
	            # Pbr Score Between 150 And 550
	        elif 550 > row['ptnrps_bad_rt_score'] > 150:
	            df.at[i,'riskcut'] = 'risk10'
	            # Received Cld In Last 6 Months
	        elif (df.at[i, 'MTH_SNC_LMTCHG'] < 6) & (df.at[i,'LMT_INC_DEC_IND'] == 'D'):
	            df.at[i, 'riskcut']='risk11'
	                
	        else:
	            df.at[i,'riskcut'] = 'risk00'
	            # Mail Flags
	            # Do Not Solicit by optouts
	            if row['mail_cd'] in ['X']:
	                df.at[i,'dmcut'] = 'dm01'
	                # Exclude Military Address
	            elif (row['CITY_0'] in ['A P O', 'APO', 'A.P.O', 'F P O', 'FPO', 'F.P.O', 'D P O', 'DPO',
	                                   'D.P.O']) | (row['ADDR_1_0'] in ['A P O', 'APO', 'A.P.O', 'F P O', 'FPO', 'F.P.O',
	                                                                  'D P O',
	                                                                  'DPO', 'D.P.O'])| (row['ADDR_2_0'] in ['A P O', 'APO',
	                                                                                                        'A.P.O',
	                                                                                                        'F P O',
	                                                                                                        'FPO', 'F.P.O',
	                                                                                                        'D P O', 'DPO',
	                                                                                                        'D.P.O']):
	                df.at[i,'dmcut'] = 'dm02'
	                # Returned Mail/Bad Address
	            elif (row['RTN_MAIL_CTR'] > 0) | (row['BLOCK_CODE_1'] in ['J']) | (row['BLOCK_CODE_2'] in ['J']):
	                df.at[i,'dmcut'] = 'dm03'
	                
	            # Foreign Countries - NOTE this includes US Territories (No Privacy Rule states we have to exclude those)
	            elif row['STATE_0'] not in ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA',
	                                        'ID', 'IL', 'IN', 'KS', 'KY',
	                                        'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH',
	                                        'NJ', 'NM', 'NV', 'NY', 'OH',
	                                        'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI',
	                                        'WV', 'WY', 'AS', 'GU', 'MP',
	                                        'PR', 'VI', 'UM']:
	                df.at[i,'dmcut'] = 'dm04'
	                
	            else:
	                df.at[i,'dmcut'] = 'dm00'
	                
	                #rtl_one['dmcut'] = dmcut
	               
	            # Email Flags
	            # Non Whitelist Purged emails
	            if (row['EMAIL_0'] == 'none@capitalone-no-email.com') | (row['EMAIL_0'] == 'none@capitalone.com'):
	                df.at[i,'emcut'] = 'em01'
	            # Enterprise Whitelist scrub
	            elif (pd.isnull(row['whtlsemail'])):
	                df.at[i, 'emcut'] = 'em02'
	            
	                                                                    
	                """                                                        
	                # Email hygiene check (bouncebacks from Kana)                                                        
	                elif 
	                 """                                                   
	                # Do Not Solicit by optouts  
	            elif row['email_cd'] in ['X']:
	                df.at[i, 'emcut'] = 'em04'
	               
	            # Exclude Military Address
	            elif (row['CITY_0'] in ['A P O', 'APO', 'A.P.O', 'F P O', 'FPO', 'F.P.O', 'D P O', 'DPO',
	                                   'D.P.O']) | (row['ADDR_1_0'] in ['A P O', 'APO', 'A.P.O', 'F P O', 'FPO', 'F.P.O',
	                                                                  'D P O',
	                                                                  'DPO', 'D.P.O'])| (row['ADDR_2_0'] in ['A P O', 'APO',
	                                                                                                        'A.P.O',
	                                                                                                        'F P O',
	                                                                                                        'FPO', 'F.P.O',
	                                                                                                        'D P O', 'DPO',
	                                                                                                        'D.P.O']):                                                        
	                df.at[i,'emcut'] ='em05'
	            # Foreign Countries - NOTE this includes US Territories (No Privacy Rule states we have to exclude those)
	            elif  row['STATE_0'] not in ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA',
	                                        'ID', 'IL', 'IN', 'KS', 'KY',
	                                        'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH',
	                                        'NJ', 'NM', 'NV', 'NY', 'OH',
	                                        'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI',
	                                        'WV', 'WY', 'AS', 'GU', 'MP',
	                                        'PR', 'VI', 'UM']:
	                df.at[i,'emcut'] = 'em06'
	            # Removal of Canadian email addresses as they have different privacy requirements
	            elif pd.isnull(row['EMAIL_0']):
	                df.at[i, 'emcut'] = 'em07' 
	            elif row['EMAIL_0'][-3:] == '.ca':
	                df.at[i, 'emcut'] = 'em08'
	                # Wireless Domains - exclude any email address which matches the wireless domain at http://transition.fcc.gov/Bureaus/CGB/DomainNames/DomainNames.txt    
	            elif row['DMN'] == row['domains']:
	                df.at[i, 'emcut'] = 'em09' 
	            
	                # Not Valid Email Address
	                #elif 
	                    #df.at[i, 'emcut'] = 'em09'
	            else:
	                df.at[i, 'emcut'] = 'em00' 
	                                                                               
	    return df           
	           
	def apply_by_multiprocessing(df, func, **kwargs):    
	    workers = kwargs.pop('workers')
	    pool = multiprocessing.Pool(processes = workers)
	    result = pool.map(func, [d for d in np.array_split(df, workers)])
	    pool.close()
	    global rtl_one
	    rtl_one = pd.concat(list(result))
	    print(len(rtl_one.index))
	    print(rtl_one.dtypes)   
	    print(rtl_one.count())
	    print(rtl_one.head(n=30))
	    # Audits
	    logger.info("Value counts on risk ")
	    logger.info(rtl_one['riskcut'].value_counts(dropna=False).sort_index())
	    logger.info("value counts for PLCC for riskpass")
	    logger.info(rtl_one[(rtl_one['SAKS_MC']!='Y') & (rtl_one['riskcut'] =='risk00' )].SAKS_MC.value_counts(dropna=False).sort_index())
	    logger.info("value counts for SAKS MC and riskpass")
	    logger.info(rtl_one[(rtl_one['SAKS_MC']=='Y') & (rtl_one['riskcut'] =='risk00')].SAKS_MC.value_counts(dropna=False).sort_index())
	    logger.info("value counts for PLCC")
	    logger.info(rtl_one[rtl_one['SAKS_MC']!='Y'].SAKS_MC.value_counts(dropna=False).sort_index())
	    logger.info("value counts for saks mc only")
	    logger.info(rtl_one[rtl_one['SAKS_MC']=='Y'].SAKS_MC.value_counts(dropna=False).sort_index())
	    logger.info("Cross tab betwenn riskcut and ORG")
	    logger.info(pd.crosstab(rtl_one.riskcut, rtl_one.ORG))
	    logger.info("Cross tab betwenn dmcut and ORG")
	    logger.info(pd.crosstab(rtl_one.dmcut, rtl_one.ORG))
	    logger.info("Cross tab betwenn emcut and ORG")
	    logger.info(pd.crosstab(rtl_one.emcut, rtl_one.ORG))
	    logger.info("Cross tab betwenn riskcut and dmcut")
	    logger.info(pd.crosstab(rtl_one.riskcut, rtl_one.dmcut))
	    logger.info("Cross tab betwenn riskcut and emcut")
	    logger.info(pd.crosstab(rtl_one.riskcut, rtl_one.emcut))
	    # Audits for Risk Criteria 
	    logger.info("Cross tab between riskcut and internal status code")
	    logger.info(pd.crosstab(rtl_one.riskcut, rtl_one.INT_STATUS))
	    logger.info("Cross tab between riskcut and Blockcode 1")
	    logger.info(pd.crosstab(rtl_one.riskcut, [rtl_one.BLOCK_CODE_1,rtl_one.BLOCK_CODE_2]))
	   
	    logger.info("Cross tab between riskcut and pgm_coll_cd ")
	    logger.info(pd.crosstab(rtl_one.riskcut, rtl_one.pgm_coll_cd))
	    #logger.info("Cross tab between riskcut and DATE_LAST_REAGE ")
	    #logger.info(pd.crosstab(rtl_one.riskcut, rtl_one.DATE_LAST_REAGE))
	    logger.info("Cross tab between riskcut and cease_desist_cd ")
	    logger.info(pd.crosstab(rtl_one.riskcut, rtl_one.cease_desist_cd))
	    logger.info("Cross tab between riskcut and coll_stat_u28_cd ")
	    logger.info(pd.crosstab(rtl_one.riskcut, rtl_one.coll_stat_u28_cd))
	    #logger.info("Cross tab between riskcut and OPEN_TO_BUY ")
	    #logger.info(pd.crosstab(rtl_one.riskcut, rtl_one.OPEN_TO_BUY))
	    logger.info("Cross tab between riskcut and dlq_day_cnt ")
	    logger.info(pd.crosstab(rtl_one.riskcut, rtl_one.dlq_day_cnt))
	    #logger.info("Cross tab between riskcut and ptnrps_bad_rt_score ")
	    #logger.info(pd.crosstab(rtl_one.riskcut, rtl_one.ptnrps_bad_rt_score))
	    logger.info("Cross tab between riskcut and ptnrps_bad_rt_score ")
	    logger.info(pd.crosstab(rtl_one.riskcut, rtl_one.ptnrps_bad_rt_score))
	    logger.info("Cross tab between riskcut and MTH_SNC_LMTCHG and LMT_INC_DEC_IND ")
	    logger.info(pd.crosstab(rtl_one.riskcut, [rtl_one.LMT_INC_DEC_IND, rtl_one.MTH_SNC_LMTCHG]))
	    
	    logger.info("=================================================MAIL AUDITS===============================================")
	    logger.info("Cross tab between riskcut, dmcut and mail_cd ")
	    logger.info(pd.crosstab(rtl_one.riskcut, [rtl_one.dmcut, rtl_one.mail_cd]))
	    logger.info("Cross tab between riskcut, dmcut  ")
	    logger.info(pd.crosstab(rtl_one.riskcut,rtl_one.dmcut ))
	    logger.info("Cross tab between riskcut, dmcut  and RTN_MAIL_CTR")
	    logger.info(pd.crosstab(rtl_one.riskcut,[rtl_one.dmcut, rtl_one.RTN_MAIL_CTR] ))
	    logger.info("Cross tab between riskcut, dmcut  and STATE_0")
	    logger.info(pd.crosstab(rtl_one.riskcut,[rtl_one.dmcut, rtl_one.STATE_0] ))
	    
	     # Value Audits
	    logger.info("=================================================Val Prop AUDITS===============================================")
	    logger.info("Value counts on Value risk ")
	    logger.info(rtl_one['valuesirk'].value_counts(dropna=False).sort_index())
	    logger.info("Cross tab betwenn valuesirk and ORG")
	    logger.info(pd.crosstab(rtl_one.valuesirk, rtl_one.ORG))
	    logger.info("Cross tab betwenn dmvalcut and ORG")
	    logger.info(pd.crosstab(rtl_one.dmvalcut, rtl_one.ORG))
	    logger.info("Cross tab betwenn emvalcut and ORG")
	    logger.info(pd.crosstab(rtl_one.emvalcut, rtl_one.ORG))
	    
	    
	        
	    
	    
	    key= path+'/'+'rtl_one'
	    key2 = 'audits.log'
	    with tempfile.TemporaryDirectory() as tempdir:
	

	        rtl_one.to_csv(os.path.join(tempdir, os.path.basename(key)), index = False, header=True, sep='\x01')
	

	        print("Uploading to {}/{}".format(s3_dest_bucket, os.path.join(s3_dest_prefix, os.path.basename(key))))
	        src_session.client('s3').upload_file(Filename=os.path.join(tempdir, os.path.basename(key)),
	                                                  Bucket=s3_dest_bucket,
	                                                  Key=os.path.join(s3_dest_prefix, key),
	                                                  ExtraArgs={"ServerSideEncryption": "AES256"})
	        
	        print("Uploading to {}/{}".format(s3_dest_bucket, os.path.join(s3_dest_prefix, os.path.basename(key2))))
	        src_session.client('s3').upload_file(Filename=key2,
	                                                  Bucket=s3_dest_bucket,
	                                                  Key=os.path.join(s3_dest_prefix, path+'/'+key2),
	                                                  ExtraArgs={"ServerSideEncryption": "AES256"})
	        
	        
	

	if __name__ == '__main__':
	    #multiprocessing.set_start_method('forkserver')
	    
	    print("getting response from source")
	    
	    NonS3Data = filedownload(key = 'retaildata/rtlone/nons3data', session = src_session)
	    ambs_one = filedownload(key = 'retaildata/rtlone/ambs_one.csv', session = src_session)
	    amna_one = filedownload(key = 'retaildata/rtlone/amna_one.csv', session = src_session)
	    
	    with tempfile.TemporaryDirectory() as tempdir:
	        
	    
	        #+++++++++++++ FOR whitelist+++++++++++++++++++++++++++
	        s3 = src_session.resource('s3')
	        filename4 = [filename.key for filename in s3.Bucket(s3_src_bucket).objects.filter(Prefix = 'retaildata/rtlraw/whitelist')]
	        filename4 = filename4[0]
	

	        #filename4='retaildata/rtlraw/whitelist/20180222_20180223020003_prtnrshp_spamhaus_ent_whtlst.dat'
	        print("downloading {}/{}".format(s3_src_bucket, filename4))
	        src_session.client('s3').download_file(Bucket=s3_src_bucket, Key=filename4,
	                                                   Filename=os.path.join(tempdir, os.path.basename(filename4)))
	        whitelist= pd.read_csv(os.path.join(tempdir, os.path.basename(filename4)), sep="\x01", names = ['whtlsemail', 'LOAD', 'whtlsemail2', 'whtlsemail3', 'ENGAGE_DATE'])
	        whitelist = whitelist.loc[:, ['whtlsemail', 'ENGAGE_DATE']]
	    
	        #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	        
	        #+++++++++++++ FOR Domain+++++++++++++++++++++++++++
	        filename5='retaildata/rtlraw/domain/domain.txt'
	        print("downloading {}/{}".format(s3_src_bucket, filename5))
	        src_session.client('s3').download_file(Bucket=s3_src_bucket, Key=filename5,
	                                                   Filename=os.path.join(tempdir, os.path.basename(filename5)))
	        domain= pd.read_csv(os.path.join(tempdir, os.path.basename(filename5)), sep="^", names = ['domain_date','domains'])
	        #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	        # Merging NonS3Data, ambs_one, amna_one
	        print(NonS3Data.dtypes)
	        print(amna_one.dtypes)
	        print(ambs_one.dtypes)
	        print(whitelist.dtypes)
	        print(domain.dtypes)
	

	        rtl_one= pd.merge(pd.merge(NonS3Data,ambs_one, left_on = ['arr_id_chain', 'ath_seq_num'],
	                               right_on =['DW_ATH_CHAIN_NBR', 'DW_ATH_SEQ_NBR']), 
	                      amna_one, left_on =['DW_ATH_CHAIN_NBR', 'DW_ATH_SEQ_NBR'],
	                      right_on =['ATH_CHAIN_NUM', 'ATH_CHAIN_SEQ_NUM'])
	        
	        rtl_one['DATE_LAST_REAGE'] = jul_to_gre(rtl_one['DATE_LAST_REAGE'])
	        rtl_one['DATE_OPENED'] = jul_to_gre(rtl_one['DATE_OPENED'])
	        rtl_one['DATE_CRLIM'] = jul_to_gre(rtl_one['DATE_CRLIM'])
	        rtl_one['DOB_0'] = jul_to_gre(rtl_one['DOB_0'])
	        # Creating the BIRTH_MONTH Variable
	        rtl_one['BIRTH_MONTH'] = rtl_one['DOB_0'].dt.month.fillna(0).astype(int)
	        rtl_one['LAST_NAME'] = rtl_one['NAME_LINE_1_0'].str.split(',').str[0]
	        rtl_one['FIRST_NAME'] = rtl_one['NAME_LINE_1_0'].str.split(',').str[1]
	

	        #Creating first name and last name variables
	        
	        rtl_one['EMAIL_0'] = rtl_one['EMAIL_0'].str.lower().replace(" ", "").str.strip() 
	        whitelist['whtlsemail'] = whitelist['whtlsemail'].str.lower().replace(" ", "").str.strip() 
	        
	        rtl_one['DMN'] = rtl_one['EMAIL_0'].str.lower().str.split("@").str[1]
	        domain['domains'] = domain['domains'].str.lower().replace(" ", "").str.strip() 
	

	        rtl_one['MAX_DATE_CRLIM_DATE_OPENED'] = rtl_one[['DATE_CRLIM', 'DATE_OPENED']].max(axis=1)
	        
	        # merging rtl_one and whitelist
	        rtl_one = pd.merge(rtl_one, whitelist, left_on= ['EMAIL_0'], right_on = ['whtlsemail'], how = 'left') 
	        # merging rtl_one and domain
	        rtl_one = pd.merge(rtl_one, domain, left_on =['DMN'], right_on = ['domains'], how = 'left').drop(['org_cd_x','org_cd_x.1', 'ORG_y', 'LOGO_y', 'ATH_CHAIN_NUM', 'ATH_CHAIN_SEQ_NUM', 'arr_id_chain', 'ath_seq_num'], axis = 1)
	        rtl_one.rename(columns={'ORG_x': 'ORG', 'LOGO_x': 'LOGO'}, inplace=True)        
	

	    df = rtl_one
	    apply_by_multiprocessing(df, risk_pass, axis =1, workers = 100)

