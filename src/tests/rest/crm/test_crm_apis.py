from django.urls import reverse
from tests.base.test_setup import BaseAPITestCase
from tests.base.assertions import DRFResponseAssertionsMixin
from tests.factories.crm_factory import CRMFactory
from tests.factories.user_factory import UserFactory
from crm.models import CRM
from tests.test_settings import test_settings
import json


@test_settings
class TestCRMGetAllLeads(BaseAPITestCase, DRFResponseAssertionsMixin):
    """Test cases for get_all_leads API endpoint"""
    
    def setUp(self):
        super().setUp()
        # Create test users
        self.user1 = UserFactory(tenant_id=self.tenant_id)
        self.user2 = UserFactory(tenant_id=self.tenant_id)
        
        # Create test CRM records
        self.crm1 = CRMFactory(user=self.user1, name="John Doe", phone_no="1234567890")
        self.crm2 = CRMFactory(user=self.user2, name="Jane Smith", phone_no="0987654321")
        self.crm3 = CRMFactory(user=self.user1, name="Bob Johnson", phone_no="5555555555")

    def test_get_all_leads_success(self):
        """Test successful retrieval of all leads"""
        url = reverse("crm:get_all_leads")
        response = self.client.get(url, **self.auth_headers)
        
        self.assert_success_response(response)
        data = response.json()
        
        # Check response structure
        self.assert_response_keys(data, ['status', 'message', 'data', 'count'])
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], 'Leads retrieved successfully')
        self.assertEqual(data['count'], 3)
        self.assert_response_list_length(data['data'], 3)

    def test_get_all_leads_with_search_filter(self):
        """Test leads retrieval with search filter"""
        url = reverse("crm:get_all_leads")
        
        # Search by name
        response = self.client.get(f"{url}?search=John", **self.auth_headers)
        self.assert_success_response(response)
        data = response.json()
        self.assertEqual(data['count'], 2)  # John Doe and Bob Johnson
        
        # Search by phone
        response = self.client.get(f"{url}?search=1234567890", **self.auth_headers)
        self.assert_success_response(response)
        data = response.json()
        self.assertEqual(data['count'], 1)
        self.assertEqual(data['data'][0]['phone_no'], '1234567890')

    def test_get_all_leads_with_user_filter(self):
        """Test leads retrieval with user filter"""
        url = reverse("crm:get_all_leads")
        response = self.client.get(f"{url}?user_id={self.user1.id}", **self.auth_headers)
        
        self.assert_success_response(response)
        data = response.json()
        self.assertEqual(data['count'], 2)  # user1 has 2 leads
        
        # Check that all leads belong to user1
        for lead in data['data']:
            self.assertEqual(lead['user_email'], self.user1.email)

    def test_get_all_leads_with_combined_filters(self):
        """Test leads retrieval with both search and user filters"""
        url = reverse("crm:get_all_leads")
        response = self.client.get(
            f"{url}?search=John&user_id={self.user1.id}", 
            **self.auth_headers
        )
        
        self.assert_success_response(response)
        data = response.json()
        self.assertEqual(data['count'], 2)  # user1 has 2 leads with "John" in name

    def test_get_all_leads_ordering(self):
        """Test that leads are ordered by created_at descending"""
        url = reverse("crm:get_all_leads")
        response = self.client.get(url, **self.auth_headers)
        
        self.assert_success_response(response)
        data = response.json()
        
        # Check that leads are ordered by created_at descending (newest first)
        created_dates = [lead['created_at'] for lead in data['data']]
        self.assertEqual(created_dates, sorted(created_dates, reverse=True))


@test_settings
class TestCRMCreateLead(BaseAPITestCase, DRFResponseAssertionsMixin):
    """Test cases for create_lead API endpoint"""
    
    def setUp(self):
        super().setUp()

    def test_create_lead_success(self):
        """Test successful lead creation"""
        url = reverse("crm:create_lead")
        lead_data = {
            'name': 'New Lead',
            'phone_no': '1111111111',
            'lead_description': 'Test lead description',
            'other_description': 'Test other description',
            'badge': 'Hot',
            'lead_creation_date': '2024-01-01'
        }
        
        response = self.client.post(
            url,
            data=json.dumps(lead_data),
            content_type='application/json',
            **self.auth_headers
        )
        
        self.assertEqual(response.status_code, 201)
        data = response.json()
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], 'Lead created successfully')
        self.assertEqual(data['data']['name'], 'New Lead')
        self.assertEqual(data['data']['phone_no'], '1111111111')
        
        # Verify lead was created in database
        self.assertTrue(CRM.objects.filter(phone_no='1111111111').exists())

    def test_create_lead_with_minimal_data(self):
        """Test lead creation with minimal required data"""
        url = reverse("crm:create_lead")
        lead_data = {
            'name': 'Minimal Lead',
            'phone_no': '2222222222'
        }
        
        response = self.client.post(
            url,
            data=json.dumps(lead_data),
            content_type='application/json',
            **self.auth_headers
        )
        
        self.assertEqual(response.status_code, 201)
        data = response.json()
        self.assertEqual(data['status'], 'success')

    def test_create_lead_duplicate_phone(self):
        """Test lead creation with duplicate phone number"""
        # First create a lead
        CRMFactory(phone_no='3333333333')
        
        url = reverse("crm:create_lead")
        lead_data = {
            'name': 'Duplicate Lead',
            'phone_no': '3333333333'  # Same as existing
        }
        
        response = self.client.post(
            url,
            data=json.dumps(lead_data),
            content_type='application/json',
            **self.auth_headers
        )
        
        self.assert_error_response(response, [400])
        data = response.json()
        self.assertEqual(data['status'], 'error')

    def test_create_lead_invalid_data(self):
        """Test lead creation with invalid data"""
        url = reverse("crm:create_lead")
        lead_data = {
            'name': '',  # Empty name
            'phone_no': 'invalid_phone'
        }
        
        response = self.client.post(
            url,
            data=json.dumps(lead_data),
            content_type='application/json',
            **self.auth_headers
        )
        
        self.assert_error_response(response, [400])
        data = response.json()
        self.assertEqual(data['status'], 'error')
        self.assertEqual(data['message'], 'Invalid data provided')


@test_settings
class TestCRMGetLeadById(BaseAPITestCase, DRFResponseAssertionsMixin):
    """Test cases for get_lead_by_id API endpoint"""
    
    def setUp(self):
        super().setUp()
        self.user = UserFactory(tenant_id=self.tenant_id)
        self.crm = CRMFactory(user=self.user, name="Test Lead", phone_no="1234567890")

    def test_get_lead_by_id_success(self):
        """Test successful retrieval of a specific lead"""
        url = reverse("crm:get_lead_by_id", kwargs={'lead_id': self.crm.id})
        response = self.client.get(url, **self.auth_headers)
        
        self.assert_success_response(response)
        data = response.json()
        
        self.assertEqual(data['status'], 'success')
        self.assertEqual(data['message'], 'Lead retrieved successfully')
        self.assertEqual(data['data']['id'], self.crm.id)
        self.assertEqual(data['data']['name'], 'Test Lead')
        self.assertEqual(data['data']['phone_no'], '1234567890')

    def test_get_lead_by_id_not_found(self):
        """Test retrieval of non-existent lead"""
        url = reverse("crm:get_lead_by_id", kwargs={'lead_id': 99999})
        response = self.client.get(url, **self.auth_headers)
        
        self.assert_error_response(response, [404])
        data = response.json()
        self.assertEqual(data['status'], 'error')
        self.assertEqual(data['message'], 'Lead not found')

    def test_get_lead_by_id_invalid_id(self):
        """Test retrieval with invalid lead ID"""
        url = reverse("crm:get_lead_by_id", kwargs={'lead_id': 'invalid'})
        response = self.client.get(url, **self.auth_headers)
        
        self.assert_error_response(response, [404])


@test_settings
class TestCRMAuthentication(BaseAPITestCase):
    """Test cases for CRM API authentication"""
    
    def setUp(self):
        super().setUp()
        self.user = UserFactory(tenant_id=self.tenant_id)
        self.crm = CRMFactory(user=self.user)

    def test_authentication_required_for_get_all_leads(self):
        """Test that authentication is required for get_all_leads"""
        url = reverse("crm:get_all_leads")
        response = self.client.get(url)
        self.assertEqual(response.status_code, 401)

    def test_authentication_required_for_create_lead(self):
        """Test that authentication is required for create_lead"""
        url = reverse("crm:create_lead")
        response = self.client.post(
            url,
            data=json.dumps({'name': 'Test', 'phone_no': '3333333333'}),
            content_type='application/json'
        )
        self.assertEqual(response.status_code, 401)

    def test_authentication_required_for_get_lead_by_id(self):
        """Test that authentication is required for get_lead_by_id"""
        url = reverse("crm:get_lead_by_id", kwargs={'lead_id': self.crm.id})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 401)


@test_settings
class TestCRMSerializers(BaseAPITestCase, DRFResponseAssertionsMixin):
    """Test cases for CRM serializers"""
    
    def setUp(self):
        super().setUp()
        self.user = UserFactory(tenant_id=self.tenant_id)
        self.crm = CRMFactory(user=self.user)

    def test_crm_serializer_fields(self):
        """Test that CRM serializer returns expected fields"""
        url = reverse("crm:get_lead_by_id", kwargs={'lead_id': self.crm.id})
        response = self.client.get(url, **self.auth_headers)
        
        self.assert_success_response(response)
        data = response.json()['data']
        
        expected_fields = [
            'id', 'name', 'phone_no', 'user', 'created_at',
            'lead_description', 'other_description', 'badge', 'lead_creation_date'
        ]
        
        for field in expected_fields:
            self.assertIn(field, data)

    def test_crm_list_serializer_fields(self):
        """Test that CRM list serializer returns expected fields"""
        url = reverse("crm:get_all_leads")
        response = self.client.get(url, **self.auth_headers)
        
        self.assert_success_response(response)
        data = response.json()['data']
        
        if data:
            lead = data[0]
            expected_fields = [
                'id', 'name', 'phone_no', 'user_email', 'created_at',
                'lead_description', 'other_description', 'badge', 'lead_creation_date'
            ]
            
            for field in expected_fields:
                self.assertIn(field, lead)
            
            # Check that user_email is present and correct
            self.assertEqual(lead['user_email'], self.user.email)
