#!/usr/bin/env python3
"""
Script to analyse data quality in dbt documentation by identifying fields with "unknown" descriptions.

This script:
1. analyses all columns*.md files in the docs folder
2. Counts total fields and fields with "unknown" descriptions per file
3. Shows a summary report with percentages
4. Lists all fields with "unknown" descriptions from columns_co.md file

Usage:
    python analyse_documentation_quality.py
"""

import os
import re
from pathlib import Path
from collections import defaultdict


def parse_docs_file(file_path):
    """
    Parse a docs markdown file and extract field documentation blocks.
    
    Args:
        file_path (str): Path to the markdown file
    
    Returns:
        dict: Dictionary with field names as keys and descriptions as values
    """
    fields = {}
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return fields
    
    # Pattern to match {% docs field_name %} ... {% enddocs %} blocks
    pattern = r'{%\s*docs\s+([^%}]+)\s*%}(.*?){%\s*enddocs\s*%}'
    matches = re.findall(pattern, content, re.DOTALL)
    
    for field_name, description in matches:
        field_name = field_name.strip()
        description = description.strip()
        fields[field_name] = description
    
    return fields


def is_unknown_description(description):
    """
    Check if a description indicates unknown/missing documentation.
    
    Args:
        description (str): The field description
    
    Returns:
        bool: True if description is considered "unknown"
    """
    description_lower = description.lower().strip()
    
    # Check for various forms of "unknown" descriptions
    unknown_indicators = [
        'unknown',
        'tbd',
        'to be defined',
        'todo',
        'missing',
        'not documented',
        'no description',
        ''
    ]
    
    return description_lower in unknown_indicators


def analyse_file_quality(file_path):
    """
    analyse documentation quality for a single file.
    
    Args:
        file_path (str): Path to the markdown file
    
    Returns:
        tuple: (total_fields, unknown_fields, unknown_field_names, file_stats)
    """
    fields = parse_docs_file(file_path)
    
    if not fields:
        return 0, 0, [], {}
    
    total_fields = len(fields)
    unknown_fields = []
    
    for field_name, description in fields.items():
        if is_unknown_description(description):
            unknown_fields.append(field_name)
    
    unknown_count = len(unknown_fields)
    documented_count = total_fields - unknown_count
    unknown_percentage = (unknown_count / total_fields * 100) if total_fields > 0 else 0
    documented_percentage = (documented_count / total_fields * 100) if total_fields > 0 else 0
    
    file_stats = {
        'total_fields': total_fields,
        'documented_fields': documented_count,
        'unknown_fields': unknown_count,
        'documented_percentage': documented_percentage,
        'unknown_percentage': unknown_percentage
    }
    
    return total_fields, unknown_count, unknown_fields, file_stats


def find_columns_files(docs_dir):
    """
    Find all columns*.md files in the docs directory.
    
    Args:
        docs_dir (Path): Path to the docs directory
    
    Returns:
        list: List of Path objects for found columns files
    """
    columns_files = []
    
    for file_path in docs_dir.glob('columns*.md'):
        columns_files.append(file_path)
    
    return sorted(columns_files)


def main():
    """Main function to execute the documentation quality analysis."""
    # Define paths
    script_dir = Path(__file__).parent
    project_dir = script_dir.parent
    docs_dir = project_dir / 'docs'
    
    print("=== dbt Documentation Quality analyser ===")
    print(f"Project directory: {project_dir}")
    print(f"Docs directory: {docs_dir}")
    
    if not docs_dir.exists():
        print(f"Error: Docs directory not found: {docs_dir}")
        return
    
    # Find all columns files
    columns_files = find_columns_files(docs_dir)
    
    if not columns_files:
        print("No columns*.md files found in the docs directory!")
        return
    
    print(f"\nFound {len(columns_files)} columns files to analyse:")
    for file_path in columns_files:
        print(f"  - {file_path.name}")
    
    print("\n" + "="*80)
    print("DOCUMENTATION QUALITY ANALYSIS")
    print("="*80)
    
    # analyse each file
    total_all_fields = 0
    total_all_unknown = 0
    co_unknown_fields = []
    
    for file_path in columns_files:
        print(f"\n?? File: {file_path.name}")
        print("-" * 60)
        
        total_fields, unknown_count, unknown_field_names, file_stats = analyse_file_quality(file_path)
        
        if total_fields == 0:
            print("   ??  No documentation blocks found or file could not be read")
            continue
        
        # Store unknown fields from columns_co.md for detailed listing
        if file_path.name == 'columns_co.md':
            co_unknown_fields = unknown_field_names
        
        # Display file statistics
        print(f"   Total fields:      {file_stats['total_fields']:>6}")
        print(f"   Documented fields: {file_stats['documented_fields']:>6} ({file_stats['documented_percentage']:>5.1f}%)")
        print(f"   Unknown fields:    {file_stats['unknown_fields']:>6} ({file_stats['unknown_percentage']:>5.1f}%)")
        
        # Quality indicator
        if file_stats['unknown_percentage'] < 10:
            quality_indicator = "?? Excellent"
        elif file_stats['unknown_percentage'] < 25:
            quality_indicator = "?? Good"
        elif file_stats['unknown_percentage'] < 50:
            quality_indicator = "?? Moderate"
        else:
            quality_indicator = "?? Needs Improvement"
        
        print(f"   Quality:           {quality_indicator}")
        
        total_all_fields += total_fields
        total_all_unknown += unknown_count
    
    # Overall summary
    print("\n" + "="*80)
    print("OVERALL SUMMARY")
    print("="*80)
    
    if total_all_fields > 0:
        overall_documented = total_all_fields - total_all_unknown
        overall_doc_percentage = (overall_documented / total_all_fields * 100)
        overall_unknown_percentage = (total_all_unknown / total_all_fields * 100)
        
        print(f"?? Total fields across all files:    {total_all_fields}")
        print(f"?? Total documented fields:          {overall_documented} ({overall_doc_percentage:.1f}%)")
        print(f"? Total unknown fields:             {total_all_unknown} ({overall_unknown_percentage:.1f}%)")
        
        if overall_unknown_percentage < 10:
            overall_quality = "?? Excellent documentation quality!"
        elif overall_unknown_percentage < 25:
            overall_quality = "?? Good documentation quality"
        elif overall_unknown_percentage < 50:
            overall_quality = "?? Moderate documentation quality - room for improvement"
        else:
            overall_quality = "?? Documentation quality needs significant improvement"
        
        print(f"\n{overall_quality}")
    
    # Detailed list of unknown fields from columns_co.md
    if co_unknown_fields:
        print("\n" + "="*80)
        print("UNKNOWN FIELDS IN columns_co.md")
        print("="*80)
        print(f"\nFound {len(co_unknown_fields)} fields with 'unknown' descriptions:")
        print()
        
        # Sort fields for better readability
        co_unknown_fields.sort()
        
        # Display in columns for better readability
        for i, field_name in enumerate(co_unknown_fields, 1):
            print(f"{i:>3}. {field_name}")
        
        print(f"\n?? These {len(co_unknown_fields)} fields in columns_co.md need proper documentation!")
        print("   Consider reaching out to domain experts to get proper descriptions.")
    else:
        print(f"\n?? Great! No unknown fields found in columns_co.md")
    
    print("\n" + "="*80)
    print("Analysis complete! Use this information to improve your dbt documentation quality.")


if __name__ == "__main__":
    main()
