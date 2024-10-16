import pandas as pd
import logging
from celery import shared_task
from .models import Transaction
from io import BytesIO
import csv
import re
from django.core.cache import cache
from django.db import transaction


# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s - %(levelname)s - %(name)s - %(module)s - '
        '%(funcName)s - line:%(lineno)d - %(process)d - '
        '%(threadName)s - %(message)s'
    ),
    handlers=(
        logging.StreamHandler(),
    )
)

logger = logging.getLogger(__name__)

BANK_CODE_MAPPING = {
    'hdfc': 101,
    'icici': 102,
    'karur_vysya': 40,
}

BANK_MAPPINGS = {
    'karur_vysya': {
        'booking': {
            'columns': ['TXN DATE', 'IRCTC ORDER NO.', 'BANK BOOKING REF.NO.', 'BOOKING AMOUNT', 'CREDITED ON'],
            'column_mapping': {
                'IRCTCORDERNO': 'Order_Id',
                'BANKBOOKINGREFNO': 'Bank_Ref_id',
                'BOOKINGAMOUNT': 'Payable_Merchant',
                'TXNDATE': 'Transaction_Date',
                'CREDITEDON': 'Settlement_Date'
            }
        },
        'refund': {
            'columns': ['REFUND DATE', 'IRCTC ORDER NO.', 'BANK BOOKING REF.NO.', 'BANK REFUND REF.NO.', 'REFUND AMOUNT', 'DEBITED ON'],
            'column_mapping': {
                'IRCTCORDERNO': 'Order_Id',
                'REFUNDAMOUNT': 'Payable_Merchant',
                'DEBITEDON': 'Settlement_Date',
                'REFUNDDATE': 'Transaction_Date',
                'BANKBOOKINGREFNO': 'Bank_Ref_id',
                'BANKREFUNDREFNO': 'Refund_Order_Id'
            }
        }
    },
    'icici': {
        'both': {
            'columns': ['POST DATE', 'FT NO.', 'SESSION ID [ASPD]', 'ARN NO', 'MID', 'TRANSACTION DATE',  'NET AMT', 'CARD NUMBER', 'CARD TYPE', 'TID'],
            'column_mapping': {
                'TRANSACTIONDATE': 'Transaction_Date',
                'SESSIONID': 'Order_Id',
                'FTNO': 'Transaction_Id',
                'ARNNO': 'Arn_No',
                'MID': 'MID',
                'POSTDATE': 'Settlement_Date',
                'NETAMT': 'Payable_Merchant',
                'CARDNUMBER': 'Card_No',
                'CARDTYPE': 'Card_type',
                'TID': 'Tid'
            }
        }
    },
}

# Precompile regex pattern for performance
column_cleaning_regex = re.compile(r'\[.*?\]')

def clean_column_name(column_name):
    logger.debug(f"Cleaning column name: '{column_name}'")
    cleaned_name = column_cleaning_regex.sub('', column_name)
    cleaned_name = ''.join(part for part in cleaned_name.split() if part)
    cleaned_name = ''.join(char for char in cleaned_name if char not in ['.', '_']).strip()
    logger.debug(f"Cleaned column name: '{cleaned_name}'")
    return cleaned_name

def convert_payable_to_float(df, column_name):
    logger.debug(f"Converting column '{column_name}' to float.")
    df[column_name] = pd.to_numeric(df[column_name].str.replace(',', ''), errors='coerce')
    return df

def convert_column_to_datetime(df, column_name):
    logger.debug(f"Converting column '{column_name}' to datetime.")
    df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
    
    # Log details of rows where datetime parsing resulted in NaT
    unsuccessful_dates = df[df[column_name].isna()]
    if not unsuccessful_dates.empty:
        logger.warning(f"Could not parse dates in column '{column_name}'; rows affected: {unsuccessful_dates.index.tolist()}")

    return df

def handle_nat_in_datetime_fields(transaction_data):
    # List all the datetime fields expected in transaction_data
    datetime_fields = ['Transaction_Date', 'Settlement_Date', 'Refund_Request_Date', 'Credit_Debit_Date', 'File_upload_Date']
    
    for field in datetime_fields:
        if transaction_data[field] is pd.NaT:
            # Log the NaT issue with specific data point reference
            logger.warning(f"Encountered NaT in {field}, setting to None or default for transaction ID: {transaction_data.get('Transaction_Id', 'Unknown')}")
            # Assign None or a specific default date as necessary
            transaction_data[field] = None

# def convert_column_to_datetime(df, column_name):
#     logger.debug(f"Converting column '{column_name}' to datetime.")
#     df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
#     return df

@shared_task
def process_uploaded_files(file_contents, bank_name, transaction_type, file_formats):
    logger.info(f"Starting process_uploaded_files with bank_name: {bank_name}, transaction_type: {transaction_type}, file_formats: {file_formats}")
    
    try:
        total_files = len(file_contents)
        logger.info(f"Total number of files to process: {total_files}")

        for file_index, (file_content, file_name) in enumerate(file_contents):
            logger.info(f"Processing file: {file_name} of type {transaction_type}")
            df_chunks = []

            try:
                if file_formats[file_index] == 'excel':
                    logger.debug(f"Reading file {file_name} as Excel.")
                    if file_name.endswith('.xlsx'):
                        logger.debug(f"Detected Excel file format: .xlsx")
                        df = pd.read_excel(BytesIO(file_content), dtype=str, engine='openpyxl')
                        df_chunks = [df]
                    elif file_name.endswith('.xls'):
                        logger.debug(f"Detected Excel file format: .xls")
                        df = pd.read_excel(BytesIO(file_content), dtype=str, engine='xlrd')
                        df_chunks = [df]
                    else:
                        logger.error(f"File {file_name} does not have a valid Excel extension.")
                elif file_formats[file_index] == 'csv':
                    logger.debug(f"Reading file {file_name} as CSV.")
                    df_chunks = pd.read_csv(BytesIO(file_content), dtype=str, quotechar='"', quoting=csv.QUOTE_MINIMAL, chunksize=50000)
                    logger.info(f"Successfully read CSV file {file_name} in chunks.")
                else:
                    logger.error(f"Unsupported file format: {file_formats[file_index]} for {file_name}")
                    raise ValueError("Unsupported file format")

                for chunk_index, df_chunk in enumerate(df_chunks):
                    logger.debug(f"Processing chunk {chunk_index + 1} of file {file_name} with shape: {df_chunk.shape}")
                    process_dataframe_chunk(df_chunk, bank_name, transaction_type)

            except Exception as e:
                logger.error(f"Error processing chunk from file: {file_name}. Exception: {e}", exc_info=True)
                continue

            logger.info(f"File processing completed for {file_name}")

        logger.info("All files have been processed.")

    except Exception as e:
        logger.error(f"Error processing files. Exception: {e}", exc_info=True)
        raise

def process_dataframe_chunk(df_chunk, bank_name, transaction_type):
    logger.info(f"Processing dataframe chunk for bank: {bank_name}, transaction type: {transaction_type}")
    cleaned_columns = [clean_column_name(col) for col in df_chunk.columns]
    df_chunk.columns = cleaned_columns
    logger.debug(f"Cleaned DataFrame columns: {df_chunk.columns.tolist()}")

    bank_mapping = BANK_MAPPINGS.get(bank_name, {})

    if transaction_type in bank_mapping:
        columns_mapping = bank_mapping[transaction_type]
    elif 'both' in bank_mapping:
        columns_mapping = bank_mapping['both']
    else:
        logger.error(f"No valid mapping found for bank: {bank_name} and transaction type: {transaction_type}")
        return

    column_mapping = columns_mapping.get('column_mapping', {})
    expected_columns = [clean_column_name(col) for col in columns_mapping.get('columns', [])]

    if all(col in df_chunk.columns for col in expected_columns):
        df_chunk.rename(columns=column_mapping, inplace=True)
        logger.debug(f"Renamed columns: {df_chunk.columns.tolist()}")

        if 'Transaction_Date' in df_chunk.columns:
            df_chunk = convert_column_to_datetime(df_chunk, 'Transaction_Date')
        if 'Settlement_Date' in df_chunk.columns:
            df_chunk = convert_column_to_datetime(df_chunk, 'Settlement_Date')
        if 'Payable_Merchant' in df_chunk.columns:
            df_chunk = convert_payable_to_float(df_chunk, 'Payable_Merchant')
            
            if bank_name == 'icici':
                df_chunk['Payable_Merchant'] = df_chunk['Payable_Merchant'].fillna(0)  # Assuming 0 is a neutral value for your logic
                df_chunk['CREDIT_DEBIT_AMOUNT'] = df_chunk['Payable_Merchant'].apply(
                    lambda x: 'CREDIT' if x > 0 else 'DEBIT' if x < 0 else None
                )
                logger.debug(f"Populated CREDIT/DEBIT AMOUNT column: {df_chunk[['Payable_Merchant', 'CREDIT_DEBIT_AMOUNT']]}")

        process_transactions(df_chunk, bank_name, transaction_type)
    else:
        missing_cols = set(expected_columns) - set(df_chunk.columns)
        logger.error(f"Missing expected columns: {missing_cols} for bank: {bank_name}, type: {transaction_type}")

def process_transactions(df_chunk, bank_name, transaction_type):
    logger.info(f"Started processing transactions for bank: {bank_name}, type: {transaction_type}")
    bulk_data_transactions = []
    seen_orders = set()
    bank_id = BANK_CODE_MAPPING.get(bank_name, None)

    if bank_id is None:
        logger.error(f"No bank ID found for bank: {bank_name}")
        return
    
    banks_with_mid_override = ['hdfc', 'icici', 'indus']

    success_count = 0
    fail_count = 0

    for index, row in df_chunk.iterrows():
        
        if transaction_type not in ['booking', 'refund', 'both']:
            logger.error(f"Unexpected transaction type: {transaction_type}. Skipping.")
            fail_count += 1
            continue

        transaction_data = {
            'Transaction_type': transaction_type,
            'Merchant_Name': row.get('Merchant_Name'),
            'MID': row.get('MID') if bank_name in banks_with_mid_override else bank_id,
            'Transaction_Id': row.get('Transaction_Id'),
            'Order_Id': row.get('Order_Id'),
            'Transaction_Date': row.get('Transaction_Date'),
            'Settlement_Date': row.get('Settlement_Date'),
            'Refund_Request_Date': convert_column_to_datetime(df_chunk, 'Refund_Request_Date') if 'Refund_Request_Date' in df_chunk else None,
            'Gross_Amount': convert_payable_to_float(df_chunk, 'Gross_Amount') if 'Gross_Amount' in df_chunk else None,
            'Aggregator_Com': convert_payable_to_float(df_chunk, 'Aggregator_Com') if 'Aggregator_Com' in df_chunk else None,
            'Acquirer_Comm': convert_payable_to_float(df_chunk, 'Acquirer_Comm') if 'Acquirer_Comm' in df_chunk else None,
            'Payable_Merchant': row.get('Payable_Merchant'),
            'Payout_from_Nodal': convert_payable_to_float(df_chunk, 'Payout_from_Nodal') if 'Payout_from_Nodal' in df_chunk else None,
            'BankName_Receive_Funds': row.get('BankName_Receive_Funds'),
            'Nodal_Account_No': row.get('Nodal_Account_No'),
            'Aggregator_Name': row.get('Aggregator_Name'),
            'Acquirer_Name': row.get('Acquirer_Name'),
            'Refund_Flag': row.get('Refund_Flag'),
            'Payments_Type': row.get('Payments_Type'),
            'MOP_Type': row.get('MOP_Type'),
            'Credit_Debit_Date': convert_column_to_datetime(df_chunk, 'Credit_Debit_Date') if 'Credit_Debit_Date' in df_chunk else None,
            'Bank_Name': bank_name,
            'Refund_Order_Id': row.get('Refund_Order_Id'),
            'Acq_Id': row.get('Acq_Id'),
            'Approve_code': row.get('Approve_code'),
            'Arn_No': row.get('Arn_No'),
            'Card_No': row.get('Card_No'),
            'Tid': row.get('Tid'),
            'Remarks': row.get('Remarks'),
            'Bank_Ref_id': row.get('Bank_Ref_id'),
            'File_upload_Date': convert_column_to_datetime(df_chunk, 'File_upload_Date') if 'File_upload_Date' in df_chunk else None,
            'User_name': row.get('User_name'),
            'Recon_Status': row.get('Recon_Status'),
            'Mpr_Summary_Trans': row.get('Mpr_Summary_Trans'),
            'Merchant_code': row.get('Merchant_code'),
            'Rec_Fmt': row.get('Rec_Fmt'),
            'Card_type': row.get('Card_type'),
            'Intl_Amount': convert_payable_to_float(df_chunk, 'Intl_Amount') if 'Intl_Amount' in df_chunk else None,
            'Domestic_Amount': convert_payable_to_float(df_chunk, 'Domestic_Amount') if 'Domestic_Amount' in df_chunk else None,
            'UDF1': None,
            'UDF2': None,
            'UDF3': None,
            'UDF4': None,
            'UDF5': None,
            'UDF6': None,
            'GST_Number': row.get('GST_Number'),
            'Credit_Debit_Amount': row.get('CREDIT_DEBIT_AMOUNT'),
        }

        handle_nat_in_datetime_fields(transaction_data)


        bulk_data_transactions.append(transaction_data)
        success_count += 1

    if bulk_data_transactions:
        logger.debug(f"Bulk inserting {len(bulk_data_transactions)} transactions.")
        try:
            with transaction.atomic():
                Transaction.bulk_create_transactions(bulk_data_transactions)
            logger.info(f"Processed {len(bulk_data_transactions)} transactions successfully.")
        # except ValueError as ve:
        #     logger.error(f"ValueError during bulk creation, possibly due to NaT issues: {ve}")
        except Exception as e:
            logger.error(f"Transaction failed: {e}. Problematic Data: {bulk_data_transactions}", exc_info=True)
            fail_count += len(bulk_data_transactions)

    logger.debug(f"Batch processing complete. Successful: {success_count}, Failed: {fail_count}")


    # Cache the results for later retrieval
    results = {
        "total_successful": success_count,
        "total_failed": fail_count,
    }
    cache.set('latest_transaction_results', results, timeout=3600) 

    return results





# import pandas as pd
# import logging
# from celery import shared_task
# from .models import Transaction
# from io import BytesIO
# import csv
# import re
# from django.core.cache import cache
# # from openpyxl import load_workbook
# # from django.db.models import Q

# # Configure logging
# logging.basicConfig(
#     level=logging.DEBUG,
#     format=(
#         '%(asctime)s - %(levelname)s - %(name)s - %(module)s - '
#         '%(funcName)s - line:%(lineno)d - %(process)d - '
#         '%(threadName)s - %(message)s'
#     ),
#     handlers=(
#         logging.StreamHandler(),
#     )
# )

# logger = logging.getLogger(__name__)

# BANK_CODE_MAPPING = {
#     'hdfc': 101,
#     'icici': 102,
#     'karur_vysya': 40,
# }

# BANK_MAPPINGS = {
#     'karur_vysya': {
#         'booking': {
#             'columns': ['TXN DATE', 'IRCTC ORDER NO.', 'BANK BOOKING REF.NO.', 'BOOKING AMOUNT', 'CREDITED ON'],
#             'column_mapping': {
#                 'IRCTCORDERNO': 'Order_Id',
#                 'BANKBOOKINGREFNO': 'Bank_Ref_id',
#                 'BOOKINGAMOUNT': 'Payable_Merchant',
#                 'TXNDATE': 'Transaction_Date',
#                 'CREDITEDON': 'Settlement_Date'
#             }
#         },
#         'refund': {
#             'columns': ['REFUND DATE', 'IRCTC ORDER NO.', 'BANK BOOKING REF.NO.', 'BANK REFUND REF.NO.', 'REFUND AMOUNT', 'DEBITED ON'],
#             'column_mapping': {
#                 'IRCTCORDERNO': 'Order_Id',
#                 'REFUNDAMOUNT': 'Payable_Merchant',
#                 'DEBITEDON': 'Settlement_Date',
#                 'REFUNDDATE': 'Transaction_Date',
#                 'BANKBOOKINGREFNO': 'Bank_Ref_id',
#                 'BANKREFUNDREFNO': 'Refund_Order_Id'
#             }
#         }
#     },
#     'icici': {
#         'both': {
#             'columns': ['POST DATE', 'FT NO.', 'SESSION ID [ASPD]', 'ARN NO', 'MID', 'TRANSACTION DATE',  'NET AMT', 'CARD NUMBER', 'CARD TYPE', 'TID'],
#             'column_mapping': {
#                 'TRANSACTIONDATE': 'Transaction_Date',
#                 'SESSIONID': 'Order_Id',
#                 'FTNO': 'Transaction_Id',
#                 'ARNNO': 'ARN_No',
#                 'MID': 'MID',
#                 'POSTDATE': 'Settlement_Date',
#                 'NETAMT': 'Payable_Merchant',
#                 'CARDNUMBER': 'Card_No',
#                 'CARDTYPE': 'Card_type',
#                 'TID': 'Tid'
#             }
#         }
#     },

#     # 'hdfc':{
#     #         'booking': {
#     #             'columns': ['TXN DATE','APPROV CODE', 'IRCTC ORDER NO.', 'BANK BOOKING REF.NO.', 'BOOKING AMOUNT', 'CREDITED ON', 'ARN NO', 'CARD NO', 'mecode', 'tid', 'SEQUENCE NUMBER'],
#     #             'column_mapping': {
#     #                 'TXNDATE': 'Transaction_Date',
#     #                 'APPROVCODE': 'Approve_code',
#     #                 'IRCTCORDERNO': 'Order_Id',
#     #                 'BANKBOOKINGREFNO': 'Transaction_Id',
#     #                 'BOOKINGAMOUNT': 'Payable_Merchant',
#     #                 'CREDITEDON': 'Settlement_Date',
#     #                 'ARNNO': 'Arn_No',
#     #                 'CARDNO': 'Card_No',
#     #                 'mecode': 'MID',
#     #                 'tid': 'Tid',
#     #                 'SEQUENCE NUMBER': 'Acq_Id'
#     #             }
#     #     }
#     # },
    
# }

# # Precompile regex pattern for performance
# column_cleaning_regex = re.compile(r'\[.*?\]')

# def clean_column_name(column_name):
#     logger.debug(f"Cleaning column name: '{column_name}'")
#     cleaned_name = column_cleaning_regex.sub('', column_name)
#     cleaned_name = ''.join(part for part in cleaned_name.split() if part)
#     cleaned_name = ''.join(char for char in cleaned_name if char not in ['.', '_']).strip()
#     logger.debug(f"Cleaned column name: '{cleaned_name}'")
#     return cleaned_name

# # def convert_to_int(val):
# #     logger.debug(f"Converting value to int: {val}")
# #     if pd.isna(val) or val == '':
# #         return None
# #     try:
# #         return int(float(val))
# #     except (ValueError, OverflowError) as e:
# #         logger.error(f"Error converting value to int: {val}. Error: {e}")
# #         return None

# def convert_payable_to_float(value):
#     logger.debug(f"Converting value to float: {value}")
#     if pd.isna(value) or value == '':
#         return None
#     value = value.replace(',', '') if isinstance(value, str) else value
#     try:
#         return float(value)
#     except (ValueError, OverflowError) as e:
#         logger.error(f"Error converting value to float: {value}. Error: {e}")
#         return None

# def convert_to_datetime(val):
#     logger.debug(f"Converting value to datetime: {val}")
#     try:
#         parsed_date = pd.to_datetime(val, errors='coerce')
#         if pd.isna(parsed_date):
#             logger.debug(f"Converted value is NaT, returning None for value: {val}")
#             return None
#         return parsed_date
#     except Exception as e:
#         logger.error(f"Error converting value to datetime: {val}. Error: {e}")
#         return None


# @shared_task
# def process_uploaded_files(file_contents, bank_name, transaction_type, file_formats):
#     logger.info(f"Starting process_uploaded_files with bank_name: {bank_name}, transaction_type: {transaction_type}, file_formats: {file_formats}")
    
#     try:
#         total_files = len(file_contents)
#         logger.info(f"Total number of files to process: {total_files}")

#         for file_index, (file_content, file_name) in enumerate(file_contents):
#             logger.info(f"Processing file: {file_name} of type {transaction_type}")
#             df_chunks = []

#             try:
#                 if file_formats[file_index] == 'excel':
#                     logger.debug(f"Reading file {file_name} as Excel.")
#                     if file_name.endswith('.xlsx'):
#                         logger.debug(f"Detected Excel file format: .xlsx")
#                         df = pd.read_excel(BytesIO(file_content), dtype=str, engine='openpyxl')
#                         df_chunks = [df]
#                     elif file_name.endswith('.xls'):
#                         logger.debug(f"Detected Excel file format: .xls")
#                         df = pd.read_excel(BytesIO(file_content), dtype=str, engine='xlrd')
#                         df_chunks = [df]
#                     else:
#                         logger.error(f"File {file_name} does not have a valid Excel extension.")
#                 elif file_formats[file_index] == 'csv':
#                     logger.debug(f"Reading file {file_name} as CSV.")
#                     df_chunks = pd.read_csv(BytesIO(file_content), dtype=str, quotechar='"', quoting=csv.QUOTE_MINIMAL, chunksize=50000)
#                     logger.info(f"Successfully read CSV file {file_name} in chunks.")
#                 else:
#                     logger.error(f"Unsupported file format: {file_formats[file_index]} for {file_name}")
#                     raise ValueError("Unsupported file format")

#                 for chunk_index, df_chunk in enumerate(df_chunks):
#                     logger.debug(f"Processing chunk {chunk_index + 1} of file {file_name} with shape: {df_chunk.shape}")
#                     process_dataframe_chunk(df_chunk, bank_name, transaction_type)

#             except Exception as e:
#                 logger.error(f"Error processing chunk from file: {file_name}. Exception: {e}", exc_info=True)
#                 continue

#             logger.info(f"File processing completed for {file_name}")

#         logger.info("All files have been processed.")

#     except Exception as e:
#         logger.error(f"Error processing files. Exception: {e}", exc_info=True)
#         raise



# def process_dataframe_chunk(df_chunk, bank_name, transaction_type):
#     logger.info(f"Processing dataframe chunk for bank: {bank_name}, transaction type: {transaction_type}")
#     cleaned_columns = [clean_column_name(col) for col in df_chunk.columns]
#     df_chunk.columns = cleaned_columns
#     logger.debug(f"Cleaned DataFrame columns: {df_chunk.columns.tolist()}")

#     bank_mapping = BANK_MAPPINGS.get(bank_name, {})

#     if transaction_type in bank_mapping:
#         columns_mapping = bank_mapping[transaction_type]
#     elif 'both' in bank_mapping:
#         columns_mapping = bank_mapping['both']
#     else:
#         logger.error(f"No valid mapping found for bank: {bank_name} and transaction type: {transaction_type}")
#         return

#     column_mapping = columns_mapping.get('column_mapping', {})
#     expected_columns = [clean_column_name(col) for col in columns_mapping.get('columns', [])]

#     if all(col in df_chunk.columns for col in expected_columns):
#         df_chunk.rename(columns=column_mapping, inplace=True)
#         logger.debug(f"Renamed columns: {df_chunk.columns.tolist()}")

#         if transaction_type == 'both' and 'Payable_Merchant' in df_chunk.columns:
#             # if transaction_type == 'both' and 'Payable_Merchant' in df_chunk.columns and bank_name == 'specific_bank_name':
#             if bank_name == 'icici':
#                 df_chunk['Payable_Merchant'] = df_chunk['Payable_Merchant'].apply(convert_payable_to_float)
#                 df_chunk['CREDIT_DEBIT_AMOUNT'] = df_chunk['Payable_Merchant'].apply(
#                     lambda x: 'CREDIT' if x > 0 else 'DEBIT' if x < 0 else None
#                 )
#                 logger.debug(f"Populated CREDIT/DEBIT AMOUNT column: {df_chunk[['Payable_Merchant', 'CREDIT_DEBIT_AMOUNT']]}")

#         process_transactions(df_chunk, bank_name, transaction_type)
#     else:
#         missing_cols = set(expected_columns) - set(df_chunk.columns)
#         logger.error(f"Missing expected columns: {missing_cols} for bank: {bank_name}, type: {transaction_type}")



# def process_transactions(df_chunk, bank_name, transaction_type):
#     logger.info(f"Started processing transactions for bank: {bank_name}, type: {transaction_type}")
#     bulk_data_transactions = []
#     seen_orders = set()
#     bank_id = BANK_CODE_MAPPING.get(bank_name, None)

#     if bank_id is None:
#         logger.error(f"No bank ID found for bank: {bank_name}")
#         return
    
#     banks_with_mid_override = ['hdfc', 'icici', 'indus']

#     success_count = 0
#     fail_count = 0

#     for index, row in df_chunk.iterrows():
        
#         if transaction_type not in ['booking', 'refund', 'both']:
#             logger.error(f"Unexpected transaction type: {transaction_type}. Skipping.")
#             fail_count += 1
#             continue


#         transaction_data = {
#             'Transaction_type': transaction_type,
#             'Merchant_Name': row.get('Merchant_Name'),
#             'MID': row.get('MID') if bank_name in banks_with_mid_override else bank_id,
#             'Transaction_Id': row.get('Transaction_Id'),
#             'Order_Id': row.get('Order_Id'),
#             'Transaction_Date': convert_to_datetime(row.get('Transaction_Date')),
#             'Settlement_Date': convert_to_datetime(row.get('Settlement_Date')),
#             'Refund_Request_Date': convert_to_datetime(row.get('Refund_Request_Date')),
#             'Gross_Amount': convert_payable_to_float(row.get('Gross_Amount')),
#             'Aggregator_Com': convert_payable_to_float(row.get('Aggregator_Com')),
#             'Acquirer_Comm': convert_payable_to_float(row.get('Acquirer_Comm')),
#             'Payable_Merchant': convert_payable_to_float(row.get('Payable_Merchant')),
#             'Payout_from_Nodal': convert_payable_to_float(row.get('Payout_from_Nodal')),
#             'BankName_Receive_Funds': row.get('BankName_Receive_Funds'),
#             'Nodal_Account_No': row.get('Nodal_Account_No'),
#             'Aggregator_Name': row.get('Aggregator_Name'),
#             'Acquirer_Name': row.get('Acquirer_Name'),
#             'Refund_Flag': row.get('Refund_Flag'),
#             'Payments_Type': row.get('Payments_Type'),
#             'MOP_Type': row.get('MOP_Type'),
#             'Credit_Debit_Date': convert_to_datetime(row.get('Credit_Debit_Date')),
#             'Bank_Name': bank_name,
#             'Refund_Order_Id': row.get('Refund_order_Id'),
#             'Acq_Id': row.get('Acq_Id'),
#             'Approve_code': row.get('Approve_code'),
#             'Arn_No': row.get('Arn_No'),
#             'Card_No': row.get('Card_No'),
#             'Tid': row.get('Tid'),
#             'Remarks': row.get('Remarks'),
#             'Bank_Ref_id': row.get('Bank_Ref_id'),
#             'File_upload_Date': convert_to_datetime(row.get('File_upload_Date')),
#             'User_name': row.get('User_name'),
#             'Recon_Status': row.get('Recon_Status'),
#             'Mpr_Summary_Trans': row.get('Mpr_Summary_Trans'),
#             'Merchant_code': row.get('Merchant_code'),
#             'Rec_Fmt': row.get('Rec_Fmt'),
#             'Card_type': row.get('Card_type'),
#             'Intl_Amount': convert_payable_to_float(row.get('Intl_Amount')),
#             'Domestic_Amount': convert_payable_to_float(row.get('Domestic_Amount')),
#             'UDF1': None,
#             'UDF2': None,
#             'UDF3': None,
#             'UDF4': None,
#             'UDF5': None,
#             'UDF6': None,
#             'GST_Number': row.get('GST_Number'),
#             'Credit_Debit_Amount': row.get('CREDIT_DEBIT_AMOUNT'),
#         }

        
#         bulk_data_transactions.append(transaction_data)
#         success_count += 1

#     if bulk_data_transactions:
#         logger.debug(f"Bulk inserting {len(bulk_data_transactions)} transactions.")
#         try:
#             Transaction.bulk_create_transactions(bulk_data_transactions)
#             logger.info(f"Processed {len(bulk_data_transactions)} transactions successfully.")
#         except Exception as e:
#             logger.error(f"Error during bulk creation of transactions: {e}", exc_info=True)
#             fail_count += len(bulk_data_transactions)

#     # Cache the results for later retrieval
#     results = {
#         "total_successful": success_count,
#         "total_failed": fail_count,
#     }
#     cache.set('latest_transaction_results', results, timeout=3600)

#     return results


# # def process_transactions(df_chunk, bank_name, transaction_type):
# #     logger.info(f"Started processing transactions for bank: {bank_name}, type: {transaction_type}")
# #     bulk_data_transactions = []
# #     seen_orders = set()
# #     bank_id = BANK_CODE_MAPPING.get(bank_name, None)

# #     if bank_id is None:
# #         logger.error(f"No bank ID found for bank: {bank_name}")
# #         return
    
# #     banks_with_mid_override = ['hdfc', 'icici', 'indus']

# #     success_count = 0
# #     fail_count = 0

# #     for index, row in df_chunk.iterrows():
# #         Order_Id = convert_to_int(row.get('Order_Id'))
# #         Transaction_Id = convert_to_int(row.get('Transaction_Id'))
# #         Acq_Id = convert_to_int(row.get('Acq_Id'))
        
# #         logger.debug(f"Processing transaction at index {index} with Order_Id: {Order_Id}")

# #         if transaction_type not in ['booking', 'refund', 'both']:
# #             logger.error(f"Unexpected transaction type: {transaction_type}. Skipping.")
# #             fail_count += 1
# #             continue


# #         transaction_data = {
# #             'Transaction_type': transaction_type,
# #             'Merchant_Name': None,
# #             'MID': row.get('MID') if bank_name in banks_with_mid_override else bank_id,
# #             'Transaction_Id': Transaction_Id,
# #             'Order_Id': Order_Id,
# #             'Transaction_Date': convert_to_datetime(row.get('Transaction_Date')),
# #             'Settlement_Date': convert_to_datetime(row.get('Settlement_Date')),
# #             'Gross_Amount': None,
# #             'Aggregator_Com': None,
# #             'Acquirer_Comm': None,
# #             'Payable_Merchant': row.get('Payable_Merchant'),
# #             'Payout_from_Nodal': None,
# #             'BankName_Receive_Funds': None,
# #             'Nodal_Account_No': None,
# #             'Aggregator_Name': None,
# #             'Acquirer_Name': None,
# #             'Refund_Flag': None,
# #             'Payments_Type': None,
# #             'MOP_Type': None,
# #             'Credit_Debit_Date': None,
# #             'Bank_Name': row.get('Acquirer_Name'), #Acquirer_Name
# #             'Refund_Order_Id': None,
# #             'Acq_Id': None,
# #             'Approve_code': None,
# #             'Arn_No': None,
# #             'Card_No': None,
# #             'Tid': None,
# #             'Remarks': None,
# #             'Bank_Ref_id': None,
# #             'File_upload_Date': None,
# #             'User_name': None,
# #             'Recon_Status': None,
# #             'Mpr_Summary_Trans': None,
# #             'Merchant_code': None,
# #             'Rec_Fmt': None,
# #             'Card_type': None,
# #             'Intl_Amount': None,
# #             'Domestic_Amount': None,
# #             'UDF1': None,
# #             'UDF2': None,
# #             'UDF3': None,
# #             'UDF4': None,
# #             'UDF5': None,
# #             'UDF6': None,
# #             'GST_Number': None,
# #             'Credit_Debit_Amount': row.get('CREDIT_DEBIT_AMOUNT'),
# #         }

# #         if transaction_data['Order_Id']:
# #             logger.debug(f"Appending transaction data for Order_Id: {transaction_data['Order_Id']}")
# #             bulk_data_transactions.append(transaction_data)
# #             seen_orders.add(Order_Id)
# #             success_count += 1

# #     if bulk_data_transactions:
# #         logger.debug(f"Bulk inserting {len(bulk_data_transactions)} transactions.")
# #         try:
# #             Transaction.bulk_create_transactions(bulk_data_transactions)
# #             logger.info(f"Processed {len(bulk_data_transactions)} transactions successfully.")
# #         except Exception as e:
# #             logger.error(f"Error during bulk creation of transactions: {e}", exc_info=True)
# #             fail_count += len(bulk_data_transactions)

# #     # Cache the results for later retrieval
# #     results = {
# #         "total_successful": success_count,
# #         "total_failed": fail_count,
# #     }
# #     cache.set('latest_transaction_results', results, timeout=3600)

# #     return results


