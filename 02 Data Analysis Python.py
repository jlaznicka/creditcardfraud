import snowflake.snowpark as snp
import snowflake.snowpark.functions as F
import snowflake.snowpark.types as T

def main(session: snp.Session): 
    # Your code goes here, inside the "main" handler.
    tableName = 'CREDIT_CARD_FRAUD.HANDS_ON.CUSTOMER_TRANSACTIONS_FRAUD'
    df_cust_trx_fraud = session.table(tableName)

    # We can use the describe function on our numeric columns to get some basic statistics.
    df_cust_trx_fraud.describe().show()
    '''
    # Number of transactions per day (PLOT ON CHART)
    df_trx_by_day = df_cust_trx_fraud.group_by(F.to_date(F.col("TX_DATETIME"))).count().sort(F.col("TO_DATE(TX_DATETIME)"))\
                                    .select(F.col("TO_DATE(TX_DATETIME)").as_("DATE"), F.col("COUNT"))

    return df_trx_by_day
    
    # Let's count the number of fraudulent and none fraudulent transactions
    df_cust_trx_fraud.group_by(F.col("TX_FRAUD")).agg(F.count(F.col("TRANSACTION_ID")).as_("NB_TX_DAY"))\
            .select(F.col("TX_FRAUD"), F.col("NB_TX_DAY"), (F.call_function("RATIO_TO_REPORT", F.col("NB_TX_DAY")).over() * 100).as_("percentage") )\
            .show()
    
    # Number of fraudlent transactions per cards (PLOT ON CHART)
    num_frd_by_card = df_cust_trx_fraud.filter(F.col("TX_FRAUD") == 1)\
                             .group_by(F.col("CUSTOMER_ID"))\
                             .agg([F.sum(F.col("TX_FRAUD")).as_("NBR_FRAUD_TRX")])
    return num_frd_by_card
    '''