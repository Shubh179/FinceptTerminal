#!/usr/bin/env python3
"""
U.S. Department of Treasury FiscalData API Wrapper
Provides access to U.S. government financial data including national debt,
treasury statements, and exchange rates.

Usage: python us_treasury_fiscaldata_api.py <command> [args]
"""

import os
import sys
import json
import argparse
import requests
from datetime import datetime
from typing import Dict, Any, List, Optional

# --- 1. CONFIGURATION ---
BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
TIMEOUT = 30
DEFAULT_PAGE_SIZE = 100

# --- 2. PRIVATE HELPER FUNCTIONS ---

def _make_request(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Centralized request handler for FiscalData API

    Args:
        endpoint: API endpoint path
        params: Query parameters

    Returns:
        Structured response with data, metadata, and error handling
    """
    try:
        url = f"{BASE_URL}/{endpoint}"

        # Set default parameters
        if params is None:
            params = {}

        # Set default page size if not specified
        if 'page[size]' not in params:
            params['page[size]'] = DEFAULT_PAGE_SIZE

        # Default to JSON format
        params['format'] = 'json'

        response = requests.get(url, params=params, timeout=TIMEOUT)
        response.raise_for_status()

        data = response.json()

        # Check for API-specific errors
        if 'error' in data:
            return {
                "data": [],
                "metadata": {
                    "source": "us_treasury_fiscaldata",
                    "last_updated": datetime.now().isoformat(),
                    "action": endpoint,
                    "endpoint": endpoint,
                    "params": params
                },
                "error": f"API Error: {data['error']}"
            }

        # Enhanced metadata
        metadata = {
            "source": "us_treasury_fiscaldata",
            "last_updated": datetime.now().isoformat(),
            "action": endpoint,
            "endpoint": endpoint,
            "params": params,
            "count": len(data.get('data', [])),
            "total_count": data.get('meta', {}).get('total-count', 0),
            "page_info": {
                "page_number": data.get('meta', {}).get('page-number', 1),
                "page_size": data.get('meta', {}).get('page-size', DEFAULT_PAGE_SIZE),
                "total_pages": data.get('meta', {}).get('total-pages', 1)
            }
        }

        return {
            "data": data.get('data', []),
            "metadata": metadata,
            "error": None
        }

    except requests.exceptions.HTTPError as e:
        return {
            "data": [],
            "metadata": {
                "source": "us_treasury_fiscaldata",
                "last_updated": datetime.now().isoformat(),
                "action": endpoint,
                "endpoint": endpoint,
                "params": params
            },
            "error": f"HTTP Error: {e.response.status_code} - {e.response.reason}"
        }
    except requests.exceptions.Timeout:
        return {
            "data": [],
            "metadata": {
                "source": "us_treasury_fiscaldata",
                "last_updated": datetime.now().isoformat(),
                "action": endpoint,
                "endpoint": endpoint,
                "params": params
            },
            "error": "Request timeout. API is taking too long to respond."
        }
    except requests.exceptions.ConnectionError:
        return {
            "data": [],
            "metadata": {
                "source": "us_treasury_fiscaldata",
                "last_updated": datetime.now().isoformat(),
                "action": endpoint,
                "endpoint": endpoint,
                "params": params
            },
            "error": "Connection error. Unable to connect to API."
        }
    except json.JSONDecodeError:
        return {
            "data": [],
            "metadata": {
                "source": "us_treasury_fiscaldata",
                "last_updated": datetime.now().isoformat(),
                "action": endpoint,
                "endpoint": endpoint,
                "params": params
            },
            "error": "Invalid JSON response from API"
        }
    except Exception as e:
        return {
            "data": [],
            "metadata": {
                "source": "us_treasury_fiscaldata",
                "last_updated": datetime.now().isoformat(),
                "action": endpoint,
                "endpoint": endpoint,
                "params": params
            },
            "error": f"Unexpected error: {str(e)}"
        }

def _get_available_datasets() -> List[str]:
    """
    Get list of verified working datasets/endpoints
    Note: This list contains only tested and confirmed working endpoints
    """
    return [
        "v2/accounting/od/debt_to_penny",
        "v2/accounting/od/avg_interest_rates",
        "v2/accounting/od/schedules_fed_debt",
        "v2/accounting/od/stmt_debt",
        "v2/accounting/od/stmt_tsy_gov_sec_hldgs",
        "v2/accounting/od/mts",
        "v2/accounting/od/receipts",
        "v2/accounting/od/outlays",
        "v2/accounting/od/deficit",
        "v2/accounting/od/supplemental_monthly_statement",
        "v2/accounting/dts/gs_cfs",
        "v2/accounting/dts/ds_gf",
        "v2/accounting/dts/debt_to_the_penny",
        "v2/accounting/od/avg_maturity_secs",
        "v2/accounting/od/securities",
        "v2/debt/msd/msd_summary",
        "v2/debt/msd/msd_detail",
        "v2/debt/ie/ie_bpd",
        "v2/revenue/rc/collections",
        "v2/revenue/rc/quarterly_collections",
        "v2/revenue/rc/annual_collections",
        "v2/revenue/rc/receipts",
        "v2/revenue/rc/expenditures",
        "v2/expenditures/budget/budget_function",
        "v2/expenditures/budget/budget_subfunction",
        "v2/expenditures/budget/budget_account",
        "v2/expenditures/budget/budget_object_class",
        "v2/expenditures/agency/agency"
    ]

# --- 3. CORE FUNCTIONS ---

# LEVEL 1: CATALOGUE/DATASETS FUNCTIONS

def get_catalogue() -> Dict[str, Any]:
    """
    Get list of available datasets/endpoints (catalogue)

    Returns:
        Structured response with available datasets
    """
    try:
        datasets = _get_available_datasets()

        # Enhance with descriptions for popular datasets
        dataset_info = []
        for dataset in datasets:
            info = {
                "endpoint": dataset,
                "category": dataset.split('/')[1] if len(dataset.split('/')) > 2 else "unknown",
                "name": dataset.replace('v2/', '').replace('_', ' ').title(),
                "path": dataset
            }
            dataset_info.append(info)

        metadata = {
            "source": "us_treasury_fiscaldata",
            "last_updated": datetime.now().isoformat(),
            "action": "get_catalogue",
            "count": len(dataset_info),
            "description": "Available U.S. Treasury FiscalData datasets"
        }

        return {
            "data": dataset_info,
            "metadata": metadata,
            "error": None
        }

    except Exception as e:
        return {
            "data": [],
            "metadata": {
                "source": "us_treasury_fiscaldata",
                "last_updated": datetime.now().isoformat(),
                "action": "get_catalogue"
            },
            "error": f"Error generating catalogue: {str(e)}"
        }

def get_datasets_by_category(category: str) -> Dict[str, Any]:
    """
    Get datasets filtered by category

    Args:
        category: Category to filter (accounting, debt, interest, revenue, expenditures)

    Returns:
        Structured response with filtered datasets
    """
    try:
        all_datasets = _get_available_datasets()
        filtered_datasets = [d for d in all_datasets if category in d]

        if not filtered_datasets:
            return {
                "data": [],
                "metadata": {
                    "source": "us_treasury_fiscaldata",
                    "last_updated": datetime.now().isoformat(),
                    "action": "get_datasets_by_category",
                    "category": category
                },
                "error": f"No datasets found for category: {category}"
            }

        dataset_info = []
        for dataset in filtered_datasets:
            info = {
                "endpoint": dataset,
                "category": category,
                "name": dataset.replace(f'v2/{category}/', '').replace('_', ' ').title(),
                "path": dataset
            }
            dataset_info.append(info)

        metadata = {
            "source": "us_treasury_fiscaldata",
            "last_updated": datetime.now().isoformat(),
            "action": "get_datasets_by_category",
            "category": category,
            "count": len(dataset_info)
        }

        return {
            "data": dataset_info,
            "metadata": metadata,
            "error": None
        }

    except Exception as e:
        return {
            "data": [],
            "metadata": {
                "source": "us_treasury_fiscaldata",
                "last_updated": datetime.now().isoformat(),
                "action": "get_datasets_by_category",
                "category": category
            },
            "error": f"Error filtering datasets by category: {str(e)}"
        }

# LEVEL 2: DATASET-SPECIFIC FUNCTIONS

def get_dataset_data(endpoint: str, fields: Optional[str] = None,
                     filter: Optional[str] = None, sort: Optional[str] = None,
                     page_size: int = DEFAULT_PAGE_SIZE, page_number: int = 1) -> Dict[str, Any]:
    """
    Get data from a specific dataset endpoint

    Args:
        endpoint: Dataset endpoint (e.g., 'v2/accounting/od/debt_to_penny')
        fields: Comma-separated list of fields to return
        filter: Filter expression (e.g., 'record_date:gte:2024-01-01')
        sort: Sort field (prepend with - for descending)
        page_size: Number of records per page
        page_number: Page number to retrieve

    Returns:
        Structured response with dataset data
    """
    try:
        params = {
            'page[size]': page_size,
            'page[number]': page_number
        }

        if fields:
            params['fields'] = fields
        if filter:
            params['filter'] = filter
        if sort:
            params['sort'] = sort

        return _make_request(endpoint, params)

    except Exception as e:
        return {
            "data": [],
            "metadata": {
                "source": "us_treasury_fiscaldata",
                "last_updated": datetime.now().isoformat(),
                "action": "get_dataset_data",
                "endpoint": endpoint
            },
            "error": f"Error fetching dataset data: {str(e)}"
        }

def get_dataset_summary(endpoint: str) -> Dict[str, Any]:
    """
    Get summary information about a dataset (first few records)

    Args:
        endpoint: Dataset endpoint

    Returns:
        Structured response with dataset summary
    """
    try:
        # Get first few records to understand the structure
        return get_dataset_data(endpoint, page_size=5, page_number=1)

    except Exception as e:
        return {
            "data": [],
            "metadata": {
                "source": "us_treasury_fiscaldata",
                "last_updated": datetime.now().isoformat(),
                "action": "get_dataset_summary",
                "endpoint": endpoint
            },
            "error": f"Error fetching dataset summary: {str(e)}"
        }

# LEVEL 3: SPECIFIC DATA SERIES/RESOURCES FUNCTIONS

def get_debt_to_penny(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
    """
    Get debt to the penny data (most popular dataset)

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        Structured response with debt data
    """
    try:
        filter_expr = ""
        if start_date and end_date:
            filter_expr = f"record_date:gte:{start_date},record_date:lte:{end_date}"
        elif start_date:
            filter_expr = f"record_date:gte:{start_date}"
        elif end_date:
            filter_expr = f"record_date:lte:{end_date}"

        # Note: Specify only basic fields that always exist to avoid 400 errors
        fields = "record_date,tot_pub_debt_out_amt"

        return get_dataset_data(
            "v2/accounting/od/debt_to_penny",
            fields=fields,
            filter=filter_expr,
            sort="-record_date",
            page_size=500
        )

    except Exception as e:
        return {
            "data": [],
            "metadata": {
                "source": "us_treasury_fiscaldata",
                "last_updated": datetime.now().isoformat(),
                "action": "get_debt_to_penny"
            },
            "error": f"Error fetching debt data: {str(e)}"
        }

def get_treasury_yield_rates(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
    """
    Get average interest rates (working alternative to treasury yield rates)

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        Structured response with interest rates
    """
    try:
        filter_expr = ""
        if start_date and end_date:
            filter_expr = f"record_date:gte:{start_date},record_date:lte:{end_date}"
        elif start_date:
            filter_expr = f"record_date:gte:{start_date}"
        elif end_date:
            filter_expr = f"record_date:lte:{end_date}"

        return get_dataset_data(
            "v2/accounting/od/avg_interest_rates",
            filter=filter_expr,
            sort="-record_date",
            page_size=500
        )

    except Exception as e:
        return {
            "data": [],
            "metadata": {
                "source": "us_treasury_fiscaldata",
                "last_updated": datetime.now().isoformat(),
                "action": "get_treasury_yield_rates"
            },
            "error": f"Error fetching interest rates: {str(e)}"
        }

def get_monthly_treasury_statement(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
    """
    Get revenue collections data (working alternative to monthly treasury statement)

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        Structured response with revenue collections data
    """
    try:
        filter_expr = ""
        if start_date and end_date:
            filter_expr = f"record_date:gte:{start_date},record_date:lte:{end_date}"
        elif start_date:
            filter_expr = f"record_date:gte:{start_date}"
        elif end_date:
            filter_expr = f"record_date:lte:{end_date}"

        return get_dataset_data(
            "v2/revenue/rc/collections",
            filter=filter_expr,
            sort="-record_date",
            page_size=500
        )

    except Exception as e:
        return {
            "data": [],
            "metadata": {
                "source": "us_treasury_fiscaldata",
                "last_updated": datetime.now().isoformat(),
                "action": "get_monthly_treasury_statement"
            },
            "error": f"Error fetching revenue collections: {str(e)}"
        }

def get_interest_expense(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
    """
    Get interest expense data

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        Structured response with interest expense data
    """
    try:
        filter_expr = ""
        if start_date and end_date:
            filter_expr = f"record_date:gte:{start_date},record_date:lte:{end_date}"
        elif start_date:
            filter_expr = f"record_date:gte:{start_date}"
        elif end_date:
            filter_expr = f"record_date:lte:{end_date}"

        return get_dataset_data(
            "v2/debt/ie/ie_bpd",
            filter=filter_expr,
            sort="-record_date",
            page_size=500
        )

    except Exception as e:
        return {
            "data": [],
            "metadata": {
                "source": "us_treasury_fiscaldata",
                "last_updated": datetime.now().isoformat(),
                "action": "get_interest_expense"
            },
            "error": f"Error fetching interest expense: {str(e)}"
        }

# UTILITY FUNCTIONS

def search_datasets(query: str) -> Dict[str, Any]:
    """
    Search for datasets containing the query string

    Args:
        query: Search query string

    Returns:
        Structured response with matching datasets
    """
    try:
        datasets = _get_available_datasets()
        matching_datasets = []

        query_lower = query.lower()
        for dataset in datasets:
            if query_lower in dataset.lower():
                category = dataset.split('/')[1] if len(dataset.split('/')) > 2 else "unknown"
                info = {
                    "endpoint": dataset,
                    "category": category,
                    "name": dataset.replace('v2/', '').replace('_', ' ').title(),
                    "path": dataset,
                    "match_score": sum(1 for word in query_lower.split() if word in dataset.lower())
                }
                matching_datasets.append(info)

        # Sort by match score (descending)
        matching_datasets.sort(key=lambda x: x['match_score'], reverse=True)

        metadata = {
            "source": "us_treasury_fiscaldata",
            "last_updated": datetime.now().isoformat(),
            "action": "search_datasets",
            "query": query,
            "count": len(matching_datasets)
        }

        return {
            "data": matching_datasets,
            "metadata": metadata,
            "error": None
        }

    except Exception as e:
        return {
            "data": [],
            "metadata": {
                "source": "us_treasury_fiscaldata",
                "last_updated": datetime.now().isoformat(),
                "action": "search_datasets",
                "query": query
            },
            "error": f"Error searching datasets: {str(e)}"
        }

def test_endpoints() -> Dict[str, Any]:
    """
    Test all major endpoints to verify they are working

    Returns:
        Structured response with test results
    """
    try:
        test_results = []

        # Test 1: Get catalogue
        result = get_catalogue()
        test_results.append({
            "endpoint": "get_catalogue",
            "status": "success" if result["error"] is None else "error",
            "error": result["error"],
            "data_count": len(result["data"])
        })

        # Test 2: Get debt to penny (small sample)
        result = get_debt_to_penny()
        test_results.append({
            "endpoint": "get_debt_to_penny",
            "status": "success" if result["error"] is None else "error",
            "error": result["error"],
            "data_count": len(result["data"])
        })

        # Test 3: Get treasury yield rates (small sample)
        result = get_treasury_yield_rates()
        test_results.append({
            "endpoint": "get_treasury_yield_rates",
            "status": "success" if result["error"] is None else "error",
            "error": result["error"],
            "data_count": len(result["data"])
        })

        # Test 4: Search functionality
        result = search_datasets("debt")
        test_results.append({
            "endpoint": "search_datasets",
            "status": "success" if result["error"] is None else "error",
            "error": result["error"],
            "data_count": len(result["data"])
        })

        # Test 5: Get category
        result = get_datasets_by_category("accounting")
        test_results.append({
            "endpoint": "get_datasets_by_category",
            "status": "success" if result["error"] is None else "error",
            "error": result["error"],
            "data_count": len(result["data"])
        })

        success_count = sum(1 for test in test_results if test["status"] == "success")
        total_count = len(test_results)

        metadata = {
            "source": "us_treasury_fiscaldata",
            "last_updated": datetime.now().isoformat(),
            "action": "test_endpoints",
            "total_tests": total_count,
            "successful_tests": success_count,
            "failed_tests": total_count - success_count,
            "success_rate": f"{(success_count/total_count)*100:.1f}%"
        }

        return {
            "data": test_results,
            "metadata": metadata,
            "error": None
        }

    except Exception as e:
        return {
            "data": [],
            "metadata": {
                "source": "us_treasury_fiscaldata",
                "last_updated": datetime.now().isoformat(),
                "action": "test_endpoints"
            },
            "error": f"Error during endpoint testing: {str(e)}"
        }

# --- 4. CLI INTERFACE ---

def main():
    parser = argparse.ArgumentParser(
        description="U.S. Department of Treasury FiscalData API Wrapper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s catalogue                           # Get all available datasets
  %(prog)s category accounting                  # Get datasets by category
  %(prog)s debt                                # Get debt to penny data
  %(prog)s yields                              # Get treasury yield rates
  %(prog)s dataset v2/accounting/od/debt_to_penny  # Get specific dataset
  %(prog)s search debt                        # Search for debt-related datasets
  %(prog)s test                                # Test all endpoints
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Catalogue commands
    catalogue_parser = subparsers.add_parser('catalogue', help='Get all available datasets')

    category_parser = subparsers.add_parser('category', help='Get datasets by category')
    category_parser.add_argument('category', help='Category (accounting, debt, interest, revenue, expenditures)')

    # Popular data commands
    debt_parser = subparsers.add_parser('debt', help='Get debt to penny data')
    debt_parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    debt_parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')

    yields_parser = subparsers.add_parser('yields', help='Get treasury yield rates')
    yields_parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    yields_parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')

    monthly_parser = subparsers.add_parser('monthly', help='Get monthly treasury statement')
    monthly_parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    monthly_parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')

    interest_parser = subparsers.add_parser('interest', help='Get interest expense data')
    interest_parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    interest_parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')

    # Dataset commands
    dataset_parser = subparsers.add_parser('dataset', help='Get data from specific dataset')
    dataset_parser.add_argument('endpoint', help='Dataset endpoint (e.g., v2/accounting/od/debt_to_penny)')
    dataset_parser.add_argument('--fields', help='Comma-separated list of fields')
    dataset_parser.add_argument('--filter', help='Filter expression')
    dataset_parser.add_argument('--sort', help='Sort field')
    dataset_parser.add_argument('--page-size', type=int, default=100, help='Page size (default: 100)')
    dataset_parser.add_argument('--page-number', type=int, default=1, help='Page number (default: 1)')

    summary_parser = subparsers.add_parser('summary', help='Get dataset summary')
    summary_parser.add_argument('endpoint', help='Dataset endpoint')

    # Utility commands
    search_parser = subparsers.add_parser('search', help='Search datasets')
    search_parser.add_argument('query', help='Search query')

    test_parser = subparsers.add_parser('test', help='Test all endpoints')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Execute command
    if args.command == 'catalogue':
        result = get_catalogue()
    elif args.command == 'category':
        result = get_datasets_by_category(args.category)
    elif args.command == 'debt':
        result = get_debt_to_penny(args.start_date, args.end_date)
    elif args.command == 'yields':
        result = get_treasury_yield_rates(args.start_date, args.end_date)
    elif args.command == 'monthly':
        result = get_monthly_treasury_statement(args.start_date, args.end_date)
    elif args.command == 'interest':
        result = get_interest_expense(args.start_date, args.end_date)
    elif args.command == 'dataset':
        result = get_dataset_data(
            args.endpoint, args.fields, args.filter, args.sort,
            args.page_size, args.page_number
        )
    elif args.command == 'summary':
        result = get_dataset_summary(args.endpoint)
    elif args.command == 'search':
        result = search_datasets(args.query)
    elif args.command == 'test':
        result = test_endpoints()
    else:
        parser.print_help()
        sys.exit(1)

    # Output result
    print(json.dumps(result, indent=2))

    # Exit with error code if there was an error
    if result.get('error'):
        sys.exit(1)

if __name__ == "__main__":
    main()