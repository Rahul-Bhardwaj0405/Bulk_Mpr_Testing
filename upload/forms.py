from django import forms

class MultipleFileInput(forms.ClearableFileInput):
    allow_multiple_selected = True

class MultipleFileField(forms.FileField):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("widget", MultipleFileInput())
        super().__init__(*args, **kwargs)

    def clean(self, data, initial=None):
        single_file_clean = super().clean
        if isinstance(data, (list, tuple)):
            result = [single_file_clean(d, initial) for d in data]
        else:
            result = single_file_clean(data, initial)
        return result

class UploadFileForm(forms.Form):
    BANK_CHOICES = [
        ('hdfc', 'HDFC'),
        ('icici', 'ICICI'),
        ('karur_vysya', 'Karur Vysya'),
    ]
    
    MERCHANT_CHOICES = [
        ('irctc_web', 'IRCTC Web'),
        ('irctc_app', 'IRCTC App'),
        ('irctc_air_ticket', 'IRCTC Air Ticket'),
        ('irctc_tourism', 'IRCTC Tourism'),
        ('all', 'All'),
    ]
    
    TRANSACTION_CHOICES = [
        ('booking', 'Booking'),
        ('refund', 'Refund'),
        ('both', 'Both'),
    ]
    
    bank_name = forms.ChoiceField(choices=BANK_CHOICES)
    merchant_name = forms.ChoiceField(choices=MERCHANT_CHOICES)
    transaction_type = forms.ChoiceField(choices=TRANSACTION_CHOICES)
    file = MultipleFileField()
