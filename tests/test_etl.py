"""Tests for the ETL pipeline.

Write at least 3 tests:
1. test_transform_filters_cancelled — cancelled orders excluded after transform
2. test_transform_filters_suspicious_quantity — quantities > 100 excluded
3. test_validate_catches_nulls — validate() raises ValueError on null customer_id
"""
import pandas as pd
import pytest
from etl_pipeline import transform, validate

def get_mock_data():
    customers = pd.DataFrame({'customer_id': [1], 'customer_name': ['Ali']})
    products = pd.DataFrame({'product_id': [101], 'product_name': ['Laptop'], 'category': ['Tech'], 'unit_price': [100]})
    orders = pd.DataFrame({'order_id': [501], 'customer_id': [1], 'status': ['shipped']})
    order_items = pd.DataFrame({'order_id': [501], 'product_id': [101], 'quantity': [1]})
    
    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items
    }
    
    
    
def test_transform_filters_cancelled():
    """Create test DataFrames with a cancelled order. Confirm it's excluded."""
    # TODO: Implement
    data = get_mock_data()
    data['orders'].loc[0, 'status'] = 'cancelled'
    
    result_df = transform(data)
    
    assert len(result_df) == 0


def test_transform_filters_suspicious_quantity():
    """Create test DataFrames with quantity > 100. Confirm it's excluded."""
    # TODO: Implement
    data = get_mock_data()

    data['order_items'].loc[0, 'quantity'] = 150
    
    result_df = transform(data)
    
    assert len(result_df) == 0


def test_validate_catches_nulls():
    """Create a DataFrame with null customer_id. Confirm validate() raises ValueError."""
    # TODO: Implement
    df_with_null = pd.DataFrame({
        'customer_id': [None], 
        'customer_name': ['Ali'],
        'total_orders': [1],
        'total_revenue': [100],
        'avg_order_value': [100]
    })
    
    with pytest.raises(ValueError):
        
        validate(df_with_null)
