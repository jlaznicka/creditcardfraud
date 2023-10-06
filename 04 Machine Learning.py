# FIRST, you need to select a DB and schema + load package called snowflake-ml-python, if packages aren't loading, select a WH in the top right corner
import snowflake.snowpark as snowpark
import snowflake.snowpark.functions as F

from snowflake.ml.modeling.linear_model import LogisticRegression
from snowflake.ml.modeling.ensemble import RandomForestClassifier

import datetime

def main(session: snowpark.Session): 

    ################################################################################################################
    # Train and Test Datasets
    ################################################################################################################
    
    #Setting dates for training and testing, we will not use the full data set for training/test
    start_date_training = datetime.datetime.strptime("2019-05-25", "%Y-%m-%d")
    delta_train = delta_delay = delta_test = 7 # Number of days in train, delay and test data sets
    end_date_data = start_date_training+datetime.timedelta(days=delta_train+delta_delay+delta_test+1)
    
    # Load data from training table
    customer_trx_fraud_dataset = session.table('CUSTOMER_TRX_FRAUD_FEATURES')

    # Get the training set data
    # Training data is not based on sampling but on a time period
    
    train_df = customer_trx_fraud_dataset.filter((F.col("TX_DATETIME") >= F.lit(start_date_training)) & (F.col("TX_DATETIME") <= F.lit(start_date_training+datetime.timedelta(days=delta_train))))
    # Get the test set data
    test_dfs = []

    # Note: Cards known to be frauded after the delay period are removed from the test set
    # That is, for each test day, all frauds known at (test_day-delay_period) are removed
    
    # First, get known frauded customers from the training set
    known_frauded_customers = train_df.filter(F.col("TX_FRAUD")==F.lit(1)).select(F.col("CUSTOMER_ID"))
    
    # Get the relative starting day of training set (easier than TX_DATETIME to collect test data)
    start_tx_time_days_training = int(train_df.select(F.min(F.col("TX_TIME_DAYS"))).to_pandas()['MIN("TX_TIME_DAYS")'].values[0])
    # Then, for each day of the test set
    # Get the customers/cards that was not known in the training data and in the delayperiod...
    for day in range(delta_test):
    
        # Get test data for one day, increased by 1 for each loop (starting with 0)
        test_df_day = customer_trx_fraud_dataset.filter(F.col("TX_TIME_DAYS") == start_tx_time_days_training+
                                                                    delta_train+delta_delay+
                                                                    day)
        
        # Frauded cards from that test day, minus the delay period, are added to the pool of known frauded customers
        test_df_day_delay_period = customer_trx_fraud_dataset.filter(F.col("TX_TIME_DAYS") == start_tx_time_days_training+
                                                                                delta_train+
                                                                                day-1)
        # fradulent customers identified during the delay period
        new_frauded_customers = test_df_day_delay_period.filter(F.col("TX_FRAUD")==F.lit(1)).select(F.col("CUSTOMER_ID"))
        
        # known_frauded_customers has fradulent customers identified in the training data
        # combine those eith fradulent customers in the delay period, remove duplicates
        known_frauded_customers = known_frauded_customers.union(new_frauded_customers)
        
        # Get the transactions for customers that is not in known_frauded_customers
        test_df_day = test_df_day.join(known_frauded_customers, test_df_day.col("CUSTOMER_ID") == known_frauded_customers.col("CUSTOMER_ID"), 'left')\
                          .filter(F.is_null(known_frauded_customers.col("CUSTOMER_ID")))\
                          .select(F.col("TRANSACTION_ID"), F.col("TX_DATETIME"), test_df_day.col("CUSTOMER_ID").alias("CUSTOMER_ID")\
                                  , F.col("TERMINAL_ID"), F.col("TX_TIME_SECONDS"), F.col("TX_TIME_DAYS"), F.col("TX_AMOUNT"), F.col("TX_FRAUD")\
                                  , F.col("TX_FRAUD_SCENARIO"), F.col("TX_DURING_WEEKEND"), F.col("TX_DURING_NIGHT"), F.col("CUST_AVG_AMOUNT_1")\
                                  , F.col("CUST_CNT_TX_1"), F.col("CUST_AVG_AMOUNT_7"), F.col("CUST_CNT_TX_7"), F.col("CUST_AVG_AMOUNT_30")\
                                  , F.col("CUST_CNT_TX_30"), F.col("NB_TX_WINDOW_1"), F.col("TERM_RISK_1"), F.col("NB_TX_WINDOW_7"), F.col("TERM_RISK_7")\
                                  , F.col("NB_TX_WINDOW_30"), F.col("TERM_RISK_30"))
        # Add it to our test data
        test_dfs.append(test_df_day)
        
    test_df = test_dfs[0].filter(F.is_null(F.col("CUSTOMER_ID")))
    for df in test_dfs:
        test_df = test_df.union(df)
    
    # Sort data sets by ascending order of transaction ID
    train_df=train_df.sort(F.col("TRANSACTION_ID"))
    test_df=test_df.sort(F.col("TRANSACTION_ID"))

    return train_df
    '''
    ################################################################################################################
    # Some simple statistics
    ################################################################################################################

    # How many rows are there in the training data set?
    train_df.select(F.count("*").alias("trainig rows total")).show()
    train_df.select(F.sum(F.col("TX_FRAUD")).alias("trainig rows fraud")).show()

    # How many rows are there in the training data set?
    test_df.select(F.count("*").alias("test rows total")).show()
    test_df.select(F.sum(F.col("TX_FRAUD")).alias("test rows fraud")).show()

    ################################################################################################################
    # Train the model
    ################################################################################################################

    # Define the features to be used
    output_feature="TX_FRAUD"

    input_features=["TX_AMOUNT","TX_DURING_WEEKEND", "TX_DURING_NIGHT", "CUST_CNT_TX_1",
       "CUST_AVG_AMOUNT_1", "CUST_CNT_TX_7", "CUST_AVG_AMOUNT_7", "CUST_CNT_TX_30",
       "CUST_AVG_AMOUNT_30", "NB_TX_WINDOW_1", "TERM_RISK_1", "NB_TX_WINDOW_7","TERM_RISK_7",
        "NB_TX_WINDOW_30","TERM_RISK_30"]
    
    # Train a model using a LogisticRegression
    lm = LogisticRegression(
        max_iter=1000,
        input_cols=input_features, 
        label_cols=output_feature, 
        output_cols=['PREDICTION_LM']
        )
    lm.fit(train_df) 
    
    # Train a model using RandomForest classifier
    rf = RandomForestClassifier(
        input_cols=input_features, 
        label_cols=output_feature, 
        output_cols=['PREDICTION_RF']
        )
    rf.fit(train_df)

    # Get the probablities for fraud for our test data set
    scored_snowml_sdf = lm.predict(test_df)
    scored_snowml_sdf = rf.predict(scored_snowml_sdf)
    
    # Saving it as a table so we do not call the predict function when using the scored DataFrame
    scored_snowml_sdf.write.save_as_table(table_name='CREDIT_CARD_FRAUD_SCORED', mode='overwrite')    
    
    # Return the test data set with predictions
    return scored_snowml_sdf
    '''
