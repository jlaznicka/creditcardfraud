# FIRST, you need to select a DB and schema + load package called snowflake-ml-python, if packages aren't loading, select a WH in the top right corner
import snowflake.snowpark as snowpark
import pandas as pd
from snowflake.ml.modeling.metrics import *

def main(session: snowpark.Session): 
    scored_sdf = session.table('CREDIT_CARD_FRAUD_SCORED')
    
    # Simple Confusion Matrix
    scored_sdf_pd = scored_sdf.to_pandas()
    cf_matrix = pd.crosstab(scored_sdf_pd['TX_FRAUD'], scored_sdf_pd['PREDICTION_LM'], rownames=['Actual'], colnames=['Predicted'])
    print("Simple Confusion Matrix LOGISTIC REGRESSION\n")
    print(cf_matrix)

    # Calculate the Accuracy, Precision, Recall and F1 metrics
    print("\nEvaluation Metrics LOGISTIC REGRESSION\n")
    print('Acccuracy:', accuracy_score(df=scored_sdf, y_true_col_names='TX_FRAUD', y_pred_col_names='PREDICTION_LM'))
    print('Precision:', precision_score(df=scored_sdf, y_true_col_names='TX_FRAUD', y_pred_col_names='PREDICTION_LM'))
    print('Recall:', recall_score(df=scored_sdf, y_true_col_names='TX_FRAUD', y_pred_col_names='PREDICTION_LM'))
    print('F1:', f1_score(df=scored_sdf, y_true_col_names='TX_FRAUD', y_pred_col_names='PREDICTION_LM'))

    # Simple Confusion Matrix
    scored_sdf_pd = scored_sdf.to_pandas()
    cf_matrix = pd.crosstab(scored_sdf_pd['TX_FRAUD'], scored_sdf_pd['PREDICTION_RF'], rownames=['Actual'], colnames=['Predicted'])
    print("\nSimple Confusion Matrix RANDOM FOREST\n")
    print(cf_matrix)

    # Calculate the Accuracy, Precision, Recall and F1 metrics
    print("\nEvaluation Metrics RANDOM FOREST\n")
    print('Acccuracy:', accuracy_score(df=scored_sdf, y_true_col_names='TX_FRAUD', y_pred_col_names='PREDICTION_RF'))
    print('Precision:', precision_score(df=scored_sdf, y_true_col_names='TX_FRAUD', y_pred_col_names='PREDICTION_RF'))
    print('Recall:', recall_score(df=scored_sdf, y_true_col_names='TX_FRAUD', y_pred_col_names='PREDICTION_RF'))
    print('F1:', f1_score(df=scored_sdf, y_true_col_names='TX_FRAUD', y_pred_col_names='PREDICTION_RF'))

