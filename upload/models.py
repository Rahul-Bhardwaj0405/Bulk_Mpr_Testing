from django.db import models, transaction
import logging

logger = logging.getLogger(__name__)

class Transaction(models.Model):
    # Fields for transaction data
    Merchant_Name = models.CharField(max_length=50, blank=True, null=True)
    MID = models.CharField(max_length=50, blank=True)
    Transaction_Id = models.CharField(max_length=50, blank=True, null=True)
    Order_Id = models.CharField(max_length=50, blank=True, null=True)
    Transaction_Date = models.DateField(blank=True, null=True)
    Settlement_Date = models.DateField()
    Refund_Request_Date = models.DateField(blank=True, null=True)
    Transaction_type = models.CharField(max_length=50, blank=True)
    Gross_Amount = models.FloatField(blank=True, null=True)
    Aggregator_Com = models.FloatField(blank=True, null=True)
    Acquirer_Comm = models.FloatField(blank=True, null=True)
    Payable_Merchant = models.FloatField(blank=True)
    Payout_from_Nodal = models.FloatField(blank=True, null=True)
    BankName_Receive_Funds = models.CharField(max_length=50, blank=True, null=True)
    Nodal_Account_No = models.CharField(max_length=50, blank=True, null=True)
    Aggregator_Name = models.CharField(max_length=50, blank=True, null=True)
    Acquirer_Name = models.CharField(max_length=50, blank=True, null=True)
    Refund_Flag = models.CharField(max_length=10, blank=True, null=True)
    Payments_Type = models.CharField(max_length=50, blank=True, null=True)
    MOP_Type = models.CharField(max_length=50, blank=True, null=True)
    Credit_Debit_Date = models.DateField(blank=True, null=True)
    Bank_Name = models.CharField(max_length=15, blank=True, null=True)
    Refund_Order_Id = models.CharField(max_length=50, blank=True, null=True)
    Acq_Id = models.CharField(max_length=125, blank=True, null=True)
    Approve_code = models.CharField(max_length=50, blank=True, null=True)
    Arn_No = models.CharField(max_length=150, blank=True, null=True)
    Card_No = models.CharField(max_length=150, blank=True, null=True)
    Tid = models.CharField(max_length=125, blank=True, null=True)
    Remarks = models.CharField(max_length=50, blank=True, null=True)
    Bank_Ref_id = models.CharField(max_length=125, blank=True, null=True)
    File_upload_Date = models.DateTimeField(blank=True, null=True)
    User_name = models.CharField(max_length=25, blank=True, null=True)
    Recon_Status = models.CharField(max_length=25, blank=True, null=True)
    Mpr_Summary_Trans = models.CharField(max_length=10, blank=True, null=True)
    Merchant_code = models.CharField(max_length=50, blank=True, null=True)
    Rec_Fmt = models.CharField(max_length=50, blank=True, null=True)
    Card_type = models.CharField(max_length=100, blank=True, null=True)
    Intl_Amount = models.FloatField(blank=True, null=True)
    Domestic_Amount = models.FloatField(blank=True, null=True)
    UDF1 = models.CharField(max_length=300, blank=True, null=True)
    UDF2 = models.CharField(max_length=300, blank=True, null=True)
    UDF3 = models.CharField(max_length=300, blank=True, null=True)
    UDF4 = models.CharField(max_length=300, blank=True, null=True)
    UDF5 = models.CharField(max_length=300, blank=True, null=True)
    UDF6 = models.CharField(max_length=300, blank=True, null=True)
    GST_Number = models.CharField(max_length=50, blank=True, null=True)
    Credit_Debit_Amount = models.CharField(max_length=6, blank=True, null=True) 


    class Meta:
        indexes = [
            models.Index(fields=['Order_Id', 'Transaction_Id']), 
            # Retain this index if it's useful for query performance but note it does not enforce uniqueness.
        ]

    def __str__(self):
        return self.Order_Id

    def clean(self):
        # Add your validation logic if needed
        pass

    @classmethod
    def bulk_create_transactions(cls, transactions):
        # Previous duplicate checking and exclusion logic based on Order_Id is removed.
        
        if transactions:
            batch_size = 1000
            try:
                for i in range(0, len(transactions), batch_size):
                    cls.objects.bulk_create(
                        [cls(**transaction) for transaction in transactions[i:i + batch_size]],
                        batch_size=batch_size,
                        ignore_conflicts=True
                    )
            except Exception as ex:
                logger.error(f"Error during bulk create: {str(ex)}")
                raise

        return len(transactions)




# from django.db import models, transaction
# import logging

# logger = logging.getLogger(__name__)

# class Transaction(models.Model):
#     # New fields replacing the common and specific fields
#     Merchant_Name = models.CharField(max_length=50, blank=True, null=True)
#     MID = models.CharField(max_length=50, blank=True)
#     Transaction_Id = models.CharField(max_length=50, blank=True, null=True)
#     Order_Id = models.CharField(max_length=50, blank=True, null=True)
#     Transaction_Date = models.DateField(blank=True, null=True)
#     Settlement_Date = models.DateField()
#     Refund_Request_Date = models.DateField(blank=True, null=True)
#     Transaction_type = models.CharField(max_length=50, blank=True)
#     Gross_Amount = models.FloatField(blank=True, null=True)
#     Aggregator_Com = models.FloatField(blank=True, null=True)
#     Acquirer_Comm = models.FloatField(blank=True, null=True)
#     Payable_Merchant = models.FloatField(blank=True)
#     Payout_from_Nodal = models.FloatField(blank=True, null=True)
#     BankName_Receive_Funds = models.CharField(max_length=50, blank=True, null=True)
#     Nodal_Account_No = models.CharField(max_length=50, blank=True, null=True)
#     Aggregator_Name = models.CharField(max_length=50, blank=True, null=True)
#     Acquirer_Name = models.CharField(max_length=50, blank=True, null=True)
#     Refund_Flag = models.CharField(max_length=10, blank=True, null=True)
#     Payments_Type = models.CharField(max_length=50, blank=True, null=True)
#     MOP_Type = models.CharField(max_length=50, blank=True, null=True)
#     Credit_Debit_Date = models.DateField(blank=True, null=True)
#     Bank_Name = models.CharField(max_length=15, blank=True, null=True)
#     Refund_Order_Id = models.CharField(max_length=50, blank=True, null=True) #cancellation id
#     Acq_Id = models.CharField(max_length=125, blank=True, null=True)
#     Approve_code = models.CharField(max_length=50, blank=True, null=True)
#     Arn_No = models.CharField(max_length=150, blank=True, null=True)
#     Card_No = models.CharField(max_length=150, blank=True, null=True)
#     Tid = models.CharField(max_length=125, blank=True, null=True)
#     Remarks = models.CharField(max_length=50, blank=True, null=True)
#     Bank_Ref_id = models.CharField(max_length=125, blank=True, null=True) #Extra data given by some banks
#     File_upload_Date = models.DateTimeField(blank=True, null=True)
#     User_name = models.CharField(max_length=25, blank=True, null=True)
#     Recon_Status = models.CharField(max_length=25, blank=True, null=True)
#     Mpr_Summary_Trans = models.CharField(max_length=10, blank=True, null=True)
#     Merchant_code = models.CharField(max_length=50, blank=True, null=True)
#     Rec_Fmt = models.CharField(max_length=50, blank=True, null=True)
#     Card_type = models.CharField(max_length=100, blank=True, null=True)
#     Intl_Amount = models.FloatField(blank=True, null=True)
#     Domestic_Amount = models.FloatField(blank=True, null=True)
#     UDF1 = models.CharField(max_length=300, blank=True, null=True)
#     UDF2= models.CharField(max_length=300, blank=True, null=True)
#     UDF3 = models.CharField(max_length=300, blank=True, null=True)
#     UDF4 = models.CharField(max_length=300, blank=True, null=True)
#     UDF5 = models.CharField(max_length=300, blank=True, null=True)
#     UDF6 = models.CharField(max_length=300, blank=True, null=True)
#     GST_Number = models.CharField(max_length=50, blank=True, null=True)
#     Credit_Debit_Amount = models.CharField(max_length=6, blank=True, null=True)  # Adjust max_length if needed


#     class Meta:
#         indexes = [
#             models.Index(fields=['Order_Id', 'Transaction_Id']),
#         ]

#     def __str__(self):
#         return self.Order_Id

#     def clean(self):
#         # Add your validation logic if needed
#         pass

#     @classmethod
#     def bulk_create_transactions(cls, transactions):
#         existing_orders = set(cls.objects.filter(
#             Order_Id__in={tx['Order_Id'] for tx in transactions if 'Order_Id' in tx}
#         ).values_list('Order_Id', flat=True))

#         new_transactions = [tx for tx in transactions if tx.get('Order_Id') not in existing_orders]

#         if new_transactions:
#             batch_size = 500
#             for i in range(0, len(new_transactions), batch_size):
#                 try:
#                     # bulk_create without transaction.atomic()
#                     cls.objects.bulk_create(
#                         [cls(**transaction) for transaction in new_transactions[i:i + batch_size]],
#                         batch_size=batch_size,
#                         ignore_conflicts=True
#                     )
#                 except Exception as ex:
#                     logger.error(f"Error during bulk create: {str(ex)}")
#                     raise

#             return len(new_transactions)
#         return 0

