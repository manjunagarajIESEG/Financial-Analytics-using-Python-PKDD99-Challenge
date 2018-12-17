############################################################################################
####################         IMPORT PACKAGES            #################################### 
############################################################################################

import pandas as pd

from datetime import datetime

import numpy as np


############################################################################################
####################         IMPORT DATASETS            #################################### 
############################################################################################

path = "C:\\Users\\nshanbhag\\Desktop\\Big Data Analytics\\Python\\group\\data_berka\\"
client = pd.read_csv(path + "client.asc",sep=";")
account = pd.read_csv(path + "account.asc",sep=";")
disp = pd.read_csv(path + "disp.asc",sep=";")
card = pd.read_csv(path + "card.asc",sep=";")
district = pd.read_csv(path + "district.asc",sep=";")
loan = pd.read_csv(path + "loan.asc",sep=";")
order = pd.read_csv(path + "order.asc",sep=";")
trans = pd.read_csv(path + "trans.asc",sep=";")



############################################################################################
####################         DEMOGRAPHIC DATASETS            ############################### 
############################################################################################

# summary of district 
summary = district.describe()    
summary = summary.transpose() 
print(summary) 



# head 
district.head(6)

# Rename the columns
district = district.rename(columns={"A1":"district_id","A2":"district_name","A3":"region","A4":"no_of_inhabitants",
                         "A5":"munci_lt_499","A6":"munci_500_1999","A7":"munci_2000_9999",
                         "A8":"munci_gt_10000","A9":"no_of_cities","A10":"ratio_of_urban_inhab",
                         "A11":"district_averagr_salary","A12":"unemployement_rate_1995","A13":"unemployement_rate_1996",
                         "A14":"prob_enterpreneur","A15":"prob_commited_crimes_1995","A16":"prob_commited_crimes_1996"})

#converted objects to numeric 
district = district.convert_objects(convert_numeric =True)

#fill nan values by mean
district = district.fillna(district.mean())

#find the probabilities
district["prob_enterpreneur"] = district["prob_enterpreneur"] / 1000
district["prob_commited_crimes_1995"] = district["prob_commited_crimes_1995"] / district["no_of_inhabitants"]
district["prob_commited_crimes_1996"] = district["prob_commited_crimes_1996"] / district["no_of_inhabitants"]

# check the data types of variables
district.dtypes                      
                
############################################################################################
####################         CLIENT DATASETS                 ############################### 
####################  WE HAVE ASSUMED CURRENT YEAR AS 1999   ###############################
############################################################################################

# Transform the birth day into year
client['birth_year'] = client['birth_number'].transform(lambda bn: int('19' + str(bn)[:2]))

# Transform the birth day into day
client['birth_day'] = client['birth_number'].transform(lambda bn: int(str(bn)[4:6]))
# Age 
client['age'] = 1999 - client['birth_year']

# Age group
client['age_group'] = client['age'] // 10 * 10

# Function to extract birth month and gender
def to_month_gender(birth_number):
    
    s = str(birth_number)
    birth_month = int(s[2:4])
    
    if birth_month > 50:
        gender = "F"
        birth_month = birth_month - 50
    else:
        gender = 'M'
        
    return pd.Series({'birth_month':birth_month, 'gender':gender})

client[['birth_month', 'gender']] = client['birth_number'].apply(to_month_gender)

# derive variable birth date
client['birth_date'] = client.apply(lambda row: datetime(row['birth_year'], row['birth_month'], row['birth_day']), axis=1)
client['birth_date'] = pd.to_datetime(client['birth_date']).dt.date

# drop unwanted columns
client = client.drop(['birth_year', 'birth_number','birth_month','birth_day'], axis=1)



############################################################################################
####################         CREDIT CARD DATASET              ############################## 
############################################################################################

card['issued'] = pd.to_datetime(card['issued']).dt.date
card = card.rename(columns = {"type":"credit_card_type","issued":"credit_card_issue_date"})

############################################################################################
####################         LOAN DATASET                     ############################## 
############################################################################################


#change the date format
loan[['date']] = loan[['date']].applymap(str).applymap(lambda s: "{}/{}/{}".format(s[4:],s[2:4],s[0:2]))

#drop loan id
loan = loan.drop(["loan_id"], axis=1)

# change the description of the field status 
loan['status'] = loan['status'].map({'A': 'Contract finished without Issue', 'B': 'Contract Finished Loan Not Paid'
                                      , 'C': 'Contract Running ok so far', 'D' : 'Contract Running Debt'})
# rename the columns
loan = loan.rename(index=str, columns={"date": "LoanIssuedDate", "amount": "LoanAmount"
                                       ,"duration": "LoanDuration","payments": "LoanPayment","status": "LoanStatus"})

    
    
############################################################################################
####################         ORDER DATASET                     ############################# 
############################################################################################

order.count()
order['account_to'].nunique()
order = order.drop(["order_id"], axis=1)
order = order.drop(["bank_to"], axis=1)
order = order.drop(["account_to"], axis=1)
order['k_symbol'].unique().tolist()


order.loc[order['k_symbol'] == " ", 'k_symbol'] = 'Other'


order = order.pivot_table(index=['account_id'],columns='k_symbol',values='amount',fill_value=0)
order.columns.name = None
order = order.reset_index()


order['account_id'].nunique()


# As the Above is also equal to 3758 = Number of records in Order so that means all Account_id is unique.


############################################################################################
####################        ACCOUNT DATASET                    ############################# 
############################################################################################

account['account_id'].nunique()

# account year tofind length of relation
account['account_year'] = account['date'].transform(lambda bn: int('19' + str(bn)[:2]))

# length of relation
account['length_of_relation'] = 1999 - account['account_year']

# drop account year
account = account.drop(['account_year'], axis=1)

account[['date']] = account[['date']].applymap(str).applymap(lambda s: "{}/{}/{}".format(s[2:4],s[4:],s[0:2]))

account['frequency'].unique().tolist()
account['frequency'] = account['frequency'].map({'POPLATEK MESICNE': 'Monthly Issuance',
                                                 'POPLATEK TYDNE'  : 'Weekly Issuance'
                                                 ,'POPLATEK PO OBRATU' : 'Issuance after Trans'})   
    
account = account.rename(index=str, columns={"date": "AccountCreatedDate"})


############################################################################################
####################       TRANSACTION DATASET                    ########################## 
############################################################################################
  
trans = trans.drop(["trans_id"], axis=1)
# Changing into Valid Dateformat
trans[['date']] = trans[['date']].applymap(str).applymap(lambda s: "{}/{}/{}".format(s[2:4],s[4:],s[0:2]))
# Dropping un neccesary details
trans = trans.drop(["account"], axis=1)
trans = trans.drop(["bank"], axis=1)
#Changing Values to more understandle ones
trans['type'] = trans['type'].map({'VYBER': "Debit",'PRIJEM':"Credit", 'VYDAJ':"Debit"})

trans['operation'] = trans['operation'].map({'VYBER KARTOU' : "Credit Card Withdrawal",
                                             'VKLAD'        : "Credit In Cash", 
                                             'PREVOD Z UCTU': "Collection from another Bank",
                                             'VYBER'        : "Withdrawal In Cash",
                                             'PREVOD NA UCET' : "Remittance to another Bank"})

trans = trans.drop(["k_symbol"], axis=1)
#Replacing nan with Other
trans = trans.replace(np.nan, "Other")
trans['amount1'] = trans.amount
#Bringing the Type as Columns to calculate Average per month later
trans = trans.pivot_table(index=['account_id','date','operation','balance','amount1'],columns='type',values='amount',fill_value=0)
trans.columns.name = None
trans = trans.reset_index()

#Pivoting Operation also as columns

trans = trans.pivot_table(index=['account_id','date','balance','Credit','Debit'],columns='operation',values='amount1',fill_value=0)
trans.columns.name = None
trans = trans.reset_index()
############## Average Month End Balances 
# Logic is For each month end to find the account Balances so from that we can get an idea about what are his savings
#at monthe end

# for that month end row is only retained along with its balance and for that the Group by condition has been applied
# to get Mean and Median Balance at Month end.
transbalance = trans[['account_id', 'date', 'balance']]
transbalance['monthyear']  = transbalance[['date']].applymap(str).applymap(lambda s: "{}{}".format(s[6:],s[0:2])) 
transbalance['day']  = transbalance[['date']].applymap(str).applymap(lambda s: "{}".format(s[3:5])) 
transbalance = transbalance.drop(["date"], axis=1)

transbalance = transbalance.sort_values(['account_id', 'monthyear','day'], ascending=[True, True,False])

def f(s):
    s2 = pd.Series(0, index=s.index)
    s2.iloc[-1] = 1
    return s2

transbalance["lastMark"] = transbalance.groupby(['account_id','monthyear'])['day'].apply(f)

transbalance = transbalance[transbalance.lastMark == 1]

transbalance = transbalance.drop(["monthyear"], axis=1)
transbalance = transbalance.drop(["day"], axis=1)
transbalance = transbalance.drop(["lastMark"], axis=1)

transbalance = transbalance.groupby('account_id', as_index=False).agg({'balance': ['mean', 'median']})

transbalance.columns  = ['account_id','Balance_Mean_per_Month','Balance_Median_per_Month']

################### 
# Here the Monthly all the different amounts are summed and after we get 1 row per month for each account id
# then Month wise averages are calculated.
# So we get an idea about his Monthly income Monthly expenditure etc from that.

transamounts = trans
transamounts = transamounts.drop(["balance"], axis=1)


transamounts['monthyear']  = transamounts[['date']].applymap(str).applymap(lambda s: "{}{}".format(s[6:],s[0:2])) 
transamounts = transamounts.drop(["date"], axis=1)

transamounts = transamounts.sort_values(['account_id', 'monthyear'], ascending=[True, True])

transamounts = transamounts.groupby(['account_id','monthyear']).sum().reset_index()

transamounts = transamounts.drop(["monthyear"], axis=1)
transamounts = transamounts.groupby(['account_id']).mean().reset_index()


trans = pd.merge(transamounts,transbalance, on=['account_id'])

# Rename columns
trans = trans.rename(columns={"Credit":"Avg_Monthly_Credit","Debit":"Avg_Monthly_Debit",
                              "Other":"Other_Transactions","Balance_Mean_per_Month":"Avg_Monthly_Balance",
                              "Median_Balance_per_Month":"Median_Monthly_Balance"})


# MONTHLY SAVINGS
trans['Avg_Monthly_Savings'] =  trans["Avg_Monthly_Credit"] - trans["Avg_Monthly_Debit"]

 
##########################################################################################
#merging all datasets
##########################################################################################

client_disp = pd.merge(client, disp, on='client_id', how='left')

client_district = pd.merge(client_disp, district, on='district_id', how='left')

client_final = pd.merge(client_district, card, on='disp_id', how='left')

account_trans = pd.merge(account, trans, on='account_id', how='left')

account_loan = pd.merge(account_trans, loan, on='account_id', how='left')

account_final = pd.merge(account_loan, order, on='account_id', how='left')

Client_Base_Table = pd.merge(client_final, account_final, on='account_id', how='left')

##########################################################################################
#Account base Table
##########################################################################################

# drop disp_id
disp = disp.drop(['disp_id'], axis=1)

# split client id as owner client id and disponent client id making account id as unique
disp = disp.pivot_table(index=['account_id'],columns='type',values='client_id',fill_value=0)
disp.columns.name = None
disp = disp.reset_index()

# DISPONENT ID EQUAL TO 0 IMPLIES THERE ARE NO DISPONENTS FOR THAT PARTICULAR ACCOUNT

# Rename columns
disp = disp.rename(columns={"DISPONENT":"Disponent_Client_ID","OWNER":"Owner_Client_ID"})

# Merge account_final with new_disp
t1 = pd.merge(account_final, disp, on='account_id', how='left')

Account_Base_Table = pd.merge(t1, district, on='district_id', how='left')

Account_Base_Table = Account_Base_Table.replace(np.nan, 0)                              
Account_Base_Table['female'] = Account_Base_Table['female'].replace(regex='female', value=1)

##########################################################################################

# cleaning NAN values
Account_Base_Table = Account_Base_Table.replace(np.nan, 0)     
Account_Base_Table['LoanStatus'].replace([0],['Not Applicable'],inplace=True)


# cleaning NAN values
Client_Base_Table = Client_Base_Table.replace(np.nan, 0)     
Client_Base_Table['LoanStatus'].replace([0],['Not Applicable'],inplace=True)
Client_Base_Table['credit_card_type'].replace([0],['Not Applicable'],inplace=True)
Client_Base_Table = Client_Base_Table.drop(["district_id_x"], axis=1)
Client_Base_Table.dtypes
