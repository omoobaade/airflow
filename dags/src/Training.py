import pandas as pd

def convert():
    Payment_code = {'Electronic check': 1, 'Mailed check': 2, 'Bank transfer (automatic)': 3,
                    'Credit card (automatic)': 4}
    df = pd.read_csv('/Users/adebolafagbule/airflow/Telco.csv')


def create_num_id():
    df = pd.read_csv('/Users/adebolafagbule/airflow/Telco.csv')
    df['Customer_Num'] = df['customerID'].apply(lambda x: x.split('-')[0])
    df['Customer_Xter'] = df['customerID'].apply(lambda x: x.split('-')[1])

    df.to_csv('/Users/adebolafagbule/airflow/updated_file.csv')

def square():
    df = pd.read_csv('/Users/adebolafagbule/airflow/updated_file.csv')
    df['square'] = df['Customer_Num'].apply(lambda x: int(x) ** 2)
    df.to_csv('/Users/adebolafagbule/airflow/updated_file_1.csv')
