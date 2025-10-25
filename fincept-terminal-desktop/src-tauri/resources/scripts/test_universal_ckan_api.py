#!/usr/bin/env python3
"""
Test Suite for Universal CKAN Data Portals API Wrapper

This script tests each endpoint of the universal_ckan_api.py to ensure
all functionality works correctly across different CKAN portals.

Usage:
    python test_universal_ckan_api.py
    python test_universal_ckan_api.py --country us  # Test specific country
    python test_universal_ckan_api.py --fast        # Quick test only
"""

import sys
import json
import time
from datetime import datetime
from typing import Dict, List, Any

# Import the API wrapper
from universal_ckan_api import (
    # Organization functions
    list_organizations,
    get_organization_details,

    # Dataset functions
    list_datasets,
    search_datasets,
    get_dataset_details,
    get_datasets_by_organization,

    # Resource functions
    get_dataset_resources,
    get_resource_details,

    # Utility functions
    get_supported_countries,
    test_portal_connection
)

# Test configuration
DEFAULT_TEST_COUNTRY = 'us'
ALL_COUNTRIES = ['us', 'uk', 'au', 'it', 'br', 'lv', 'si', 'uy']

class Colors:
    """Terminal color codes for test output."""
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'

def print_success(message: str):
    """Print success message in green."""
    print(f"{Colors.GREEN}[PASS] {message}{Colors.END}")

def print_error(message: str):
    """Print error message in red."""
    print(f"{Colors.RED}[FAIL] {message}{Colors.END}")

def print_warning(message: str):
    """Print warning message in yellow."""
    print(f"{Colors.YELLOW}[WARN] {message}{Colors.END}")

def print_info(message: str):
    """Print info message in blue."""
    print(f"{Colors.BLUE}[INFO] {message}{Colors.END}")

def print_header(message: str):
    """Print header message in bold."""
    print(f"\n{Colors.BOLD}{message}{Colors.END}")

def test_function(test_name: str, test_func, *args, **kwargs) -> bool:
    """
    Test a single function and return success status.

    Args:
        test_name: Name of the test
        test_func: Function to test
        *args: Function arguments
        **kwargs: Function keyword arguments

    Returns:
        True if test passed, False otherwise
    """
    print(f"  Testing {test_name}... ", end='', flush=True)

    try:
        start_time = time.time()
        result = test_func(*args, **kwargs)
        end_time = time.time()

        # Check if result is valid
        if not isinstance(result, dict):
            print_error(f"Invalid result type: {type(result)}")
            return False

        if 'error' not in result or 'data' not in result or 'metadata' not in result:
            print_error("Missing required fields (data, metadata, error)")
            return False

        # Check for API errors
        if result['error']:
            print_error(f"API Error: {result['error']}")
            return False

        # Validate metadata
        metadata = result['metadata']
        required_metadata = ['source', 'country', 'last_updated', 'count']
        for field in required_metadata:
            if field not in metadata:
                print_error(f"Missing metadata field: {field}")
                return False

        execution_time = round((end_time - start_time) * 1000)
        data_count = metadata.get('count', 0)

        print_success(f"OK ({execution_time}ms, {data_count} items)")
        return True

    except Exception as e:
        print_error(f"Exception: {str(e)}")
        return False

def test_supported_countries():
    """Test the get_supported_countries function."""
    print_header("Testing Supported Countries Function")

    success = test_function(
        "get_supported_countries",
        get_supported_countries
    )

    if success:
        result = get_supported_countries()
        countries = result['data']
        print_info(f"Found {len(countries)} supported countries")
        for country in countries:
            print(f"    - {country['code']}: {country['name']}")

    return success

def test_portal_connections(countries: List[str], fast_mode: bool = False):
    """Test portal connections for specified countries."""
    print_header("Testing Portal Connections")

    if fast_mode:
        print_info("Fast mode: Testing only primary countries")
        test_countries = ['us', 'uk', 'au']
    else:
        test_countries = countries

    results = {}

    for country in test_countries:
        print(f"\nTesting {country.upper()} portal:")

        success = test_function(
            f"test_portal_connection({country})",
            test_portal_connection,
            country
        )

        results[country] = success

        if not success:
            print_warning(f"Skipping {country.upper()} due to connection failure")

    return results

def test_organization_functions(countries: List[str], fast_mode: bool = False):
    """Test organization-related functions."""
    print_header("Testing Organization Functions")

    test_countries = countries[:2] if fast_mode else countries
    organization_results = {}

    for country in test_countries:
        print(f"\nTesting organization functions for {country.upper()}:")

        # Test list_organizations
        success1 = test_function(
            f"list_organizations({country})",
            list_organizations,
            country,
            5  # Limit to 5 for testing
        )

        # Get an organization name for detailed testing
        org_details_success = False
        if success1:
            try:
                org_result = list_organizations(country, 1)
                if not org_result['error'] and org_result['data']:
                    org_name = org_result['data'][0]['name']
                    org_details_success = test_function(
                        f"get_organization_details({country}, {org_name})",
                        get_organization_details,
                        country,
                        org_name
                    )
            except Exception:
                pass

        organization_results[country] = success1 and org_details_success

    return organization_results

def test_dataset_functions(countries: List[str], fast_mode: bool = False):
    """Test dataset-related functions."""
    print_header("Testing Dataset Functions")

    test_countries = countries[:2] if fast_mode else countries
    dataset_results = {}

    for country in test_countries:
        print(f"\nTesting dataset functions for {country.upper()}:")

        # Test list_datasets
        success1 = test_function(
            f"list_datasets({country})",
            list_datasets,
            country,
            10  # Limit to 10 for testing
        )

        # Test search_datasets
        success2 = test_function(
            f"search_datasets({country}, 'climate')",
            search_datasets,
            country,
            "climate",
            5  # Limit to 5 for testing
        )

        # Test get_dataset_details (use a dataset from search)
        details_success = False
        if success2:
            try:
                search_result = search_datasets(country, "climate", 1)
                if not search_result['error'] and search_result['data']:
                    dataset_name = search_result['data'][0]['name']
                    details_success = test_function(
                        f"get_dataset_details({country}, {dataset_name})",
                        get_dataset_details,
                        country,
                        dataset_name
                    )
            except Exception:
                pass

        dataset_results[country] = success1 and success2 and details_success

    return dataset_results

def test_resource_functions(countries: List[str], fast_mode: bool = False):
    """Test resource-related functions."""
    print_header("Testing Resource Functions")

    test_countries = countries[:1] if fast_mode else countries
    resource_results = {}

    for country in test_countries:
        print(f"\nTesting resource functions for {country.upper()}:")

        # Get a dataset for resource testing
        dataset_name = None
        try:
            search_result = search_datasets(country, "data", 1)
            if not search_result['error'] and search_result['data']:
                dataset_name = search_result['data'][0]['name']
        except Exception:
            pass

        if not dataset_name:
            print_warning(f"Could not find dataset for resource testing in {country.upper()}")
            resource_results[country] = False
            continue

        # Test get_dataset_resources
        success1 = test_function(
            f"get_dataset_resources({country}, {dataset_name})",
            get_dataset_resources,
            country,
            dataset_name
        )

        # Test get_resource_details (use a resource from the dataset)
        details_success = False
        if success1:
            try:
                resources_result = get_dataset_resources(country, dataset_name)
                if not resources_result['error'] and resources_result['data']:
                    resource_id = resources_result['data'][0]['id']
                    details_success = test_function(
                        f"get_resource_details({country}, {resource_id})",
                        get_resource_details,
                        country,
                        resource_id
                    )
            except Exception:
                pass

        resource_results[country] = success1 and details_success

    return resource_results

def test_integrated_workflow(countries: List[str]):
    """Test an integrated workflow using multiple functions."""
    print_header("Testing Integrated Workflow")

    # Test with US portal for comprehensive workflow
    country = 'us'
    print(f"\nTesting integrated workflow for {country.upper()}:")

    try:
        # Step 1: Search for climate datasets
        print("  Step 1: Searching for climate datasets...")
        search_result = search_datasets(country, "climate", 3)
        if search_result['error']:
            print_error(f"Search failed: {search_result['error']}")
            return False

        datasets = search_result['data']
        if not datasets:
            print_warning("No climate datasets found")
            return False

        print_success(f"Found {len(datasets)} climate datasets")

        # Step 2: Get details for first dataset
        print("  Step 2: Getting dataset details...")
        dataset_name = datasets[0]['name']
        details_result = get_dataset_details(country, dataset_name)
        if details_result['error']:
            print_error(f"Dataset details failed: {details_result['error']}")
            return False

        dataset_details = details_result['data']
        print_success(f"Got details for '{dataset_details['title']}'")

        # Step 3: Get resources for the dataset
        print("  Step 3: Getting dataset resources...")
        resources_result = get_dataset_resources(country, dataset_name)
        if resources_result['error']:
            print_error(f"Resources failed: {resources_result['error']}")
            return False

        resources = resources_result['data']
        print_success(f"Found {len(resources)} resources")

        # Step 4: Get organization info
        print("  Step 4: Getting organization information...")
        org_name = dataset_details.get('organization', {}).get('name')
        if org_name:
            org_result = get_organization_details(country, org_name)
            if org_result['error']:
                print_warning(f"Organization details failed: {org_result['error']}")
            else:
                print_success(f"Got organization '{org_result['data']['title']}'")

        print_success("Integrated workflow completed successfully!")
        return True

    except Exception as e:
        print_error(f"Integrated workflow failed: {str(e)}")
        return False

def run_all_tests(countries: List[str] = None, fast_mode: bool = False):
    """
    Run all tests and return overall results.

    Args:
        countries: List of countries to test (default: all)
        fast_mode: Run quick tests only
    """
    print_header("Universal CKAN API Wrapper - Test Suite")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Fast mode: {'Yes' if fast_mode else 'No'}")

    if countries is None:
        countries = ALL_COUNTRIES

    print(f"Testing countries: {', '.join([c.upper() for c in countries])}")

    start_time = time.time()
    test_results = {}

    # Run individual test suites
    test_results['supported_countries'] = test_supported_countries()

    connection_results = test_portal_connections(countries, fast_mode)
    test_results['connections'] = connection_results

    # Filter out countries that failed connection tests
    working_countries = [c for c, success in connection_results.items() if success]

    if working_countries:
        test_results['organizations'] = test_organization_functions(working_countries, fast_mode)
        test_results['datasets'] = test_dataset_functions(working_countries, fast_mode)
        test_results['resources'] = test_resource_functions(working_countries, fast_mode)
        test_results['integrated_workflow'] = test_integrated_workflow(working_countries)
    else:
        print_warning("No working connections found, skipping remaining tests")
        test_results['organizations'] = {}
        test_results['datasets'] = {}
        test_results['resources'] = {}
        test_results['integrated_workflow'] = False

    end_time = time.time()

    # Print summary
    print_header("Test Summary")
    total_time = round(end_time - start_time, 2)

    print(f"Total execution time: {total_time} seconds")
    print(f"Tests started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Calculate success rates
    connection_success_rate = sum(connection_results.values()) / len(connection_results) * 100
    print(f"Connection success rate: {connection_success_rate:.1f}%")

    if working_countries:
        org_success_rate = sum(test_results['organizations'].values()) / len(test_results['organizations']) * 100
        dataset_success_rate = sum(test_results['datasets'].values()) / len(test_results['datasets']) * 100
        resource_success_rate = sum(test_results['resources'].values()) / len(test_results['resources']) * 100

        print(f"Organization function success rate: {org_success_rate:.1f}%")
        print(f"Dataset function success rate: {dataset_success_rate:.1f}%")
        print(f"Resource function success rate: {resource_success_rate:.1f}%")

    workflow_status = "PASS" if test_results['integrated_workflow'] else "FAIL"
    print(f"Integrated workflow: {workflow_status}")

    # Overall assessment
    print_header("Overall Assessment")

    if connection_success_rate == 100 and test_results.get('integrated_workflow', False):
        print_success("All tests passed! The API wrapper is working correctly.")
        return True
    elif connection_success_rate >= 50:
        print_warning("Some tests failed. The API wrapper is partially working.")
        return False
    else:
        print_error("Most tests failed. There may be network issues or API problems.")
        return False

def main():
    """Main test execution function."""
    # Parse command line arguments
    countries = None
    fast_mode = '--fast' in sys.argv

    if '--country' in sys.argv:
        try:
            country_index = sys.argv.index('--country') + 1
            if country_index < len(sys.argv):
                countries = [sys.argv[country_index].lower()]
                if countries[0] not in ALL_COUNTRIES:
                    print_error(f"Invalid country: {countries[0]}")
                    print(f"Valid countries: {', '.join(ALL_COUNTRIES)}")
                    sys.exit(1)
        except (ValueError, IndexError):
            print_error("Invalid --country argument")
            sys.exit(1)

    # Run tests
    success = run_all_tests(countries, fast_mode)

    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()