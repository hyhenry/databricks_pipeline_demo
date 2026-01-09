# Databricks notebook source
# MAGIC %md
# MAGIC # Linear to Databricks ELT Pipeline
# MAGIC
# MAGIC This notebook extracts data from Linear GraphQL API and loads it into Unity Catalog tables
# MAGIC with a flattened structure.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Installation and Setup

# COMMAND ----------

# Import libraries
import requests
import json

import pandas as pd
from datetime import datetime, timedelta, UTC
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
import uuid
from typing import List, Dict, Any

# COMMAND ----------

# MAGIC %run ../../../utilities/write_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.dropdown("load_type", "incremental", ["full", "incremental"])
dbutils.widgets.text("env","development") #set default environment to dev

load_type = dbutils.widgets.get("load_type")

LINEAR_API_KEY = dbutils.secrets.get(scope="linear", key="api_key")
LINEAR_API_URL = "https://api.linear.app/graphql"

# Unity Catalog configuration
is_dev = dbutils.widgets.get("env") == "development"
CATALOG_NAME = "bronze"
SCHEMA_NAME = "linear"

if is_dev:
    CATALOG_NAME = f"{CATALOG_NAME}_dev"
    print(f"running in dev environment, saving data to {CATALOG_NAME}.{SCHEMA_NAME}")
else:
    print(f"running in prd environment, saving data to {CATALOG_NAME}.{SCHEMA_NAME}")

FULL_SCHEMA_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}"

# Create schema if it doesn't exist
schema_exists = spark.sql(f"SHOW SCHEMAS IN {CATALOG_NAME}").filter(col("databaseName") == SCHEMA_NAME).count() > 0
if not schema_exists:
    print("Schema doesn't exist, creating now...")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA_NAME};")
    load_type = "full" #enforce a full load

# COMMAND ----------

# MAGIC %md
# MAGIC ## Linear GraphQL Client

# COMMAND ----------

class LinearGraphQLClient:
    def __init__(self, api_key: str, api_url: str):
        self.api_key = api_key
        self.api_url = api_url
        self.headers = {
            "Authorization": f"{api_key}",
            "Content-Type": "application/json"
        }
    
    def execute_query(self, query: str, variables: dict = None):
        """Execute GraphQL query against Linear API"""
        payload = {
            "query": query,
            "variables": variables or {}
        }
        
        response = requests.post(
            self.api_url, 
            headers=self.headers, 
            json=payload
        )
        
        if response.status_code != 200:
            raise Exception(f"GraphQL query failed: {response.status_code} - {response.text}")
        
        result = response.json()
        if "errors" in result:
            raise Exception(f"GraphQL errors: {result['errors']}")
        
        return result["data"]
    
    def paginated_query(self, query: str, variables: dict = None, page_size: int = 100):
        """Execute paginated GraphQL query"""
        all_data = []
        has_next_page = True
        after_cursor = None
        
        while has_next_page:
            current_variables = {**(variables or {})}
            current_variables["first"] = page_size
            if after_cursor:
                current_variables["after"] = after_cursor
            
            data = self.execute_query(query, current_variables)
            
            # Extract the main collection (assumes structure like {collection: {nodes: [], pageInfo: {}}})
            collection_key = next(iter(data.keys()))
            collection = data[collection_key]
            
            all_data.extend(collection["nodes"])
            
            page_info = collection.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            after_cursor = page_info.get("endCursor")
        
        return all_data

# Initialize client
linear_client = LinearGraphQLClient(LINEAR_API_KEY, LINEAR_API_URL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## GraphQL Queries

# COMMAND ----------

# GraphQL Queries for different Linear entities
QUERIES = {
    "teams": """
        query GetTeams($first: Int, $after: String) {
            teams(first: $first, after: $after) {
                nodes {
                    id
                    name
                    key
                    description
                    color
                    icon
                    private
                    archivedAt
                    createdAt
                    updatedAt
                    organization {
                        id
                        name
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
    """,
    
    "users": """
        query GetUsers($first: Int, $after: String) {
            users(first: $first, after: $after) {
                nodes {
                    id
                    name
                    displayName
                    email
                    avatarUrl
                    active
                    admin
                    guest
                    createdAt
                    updatedAt
                    archivedAt
                    timezone
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
    """,
    
    "issues": """
        query GetIssues($first: Int, $after: String, $filter: IssueFilter) {
            issues(first: $first, after: $after, filter: $filter) {
                nodes {
                    id
                    identifier
                    title
                    description
                    priority
                    estimate
                    branchName
                    customerTicketCount
                    cycle {
                        id
                        name
                    }
                    project {
                        id
                        name
                    }
                    team {
                        id
                        name
                        key
                    }
                    assignee {
                        id
                        name
                        email
                    }
                    creator {
                        id
                        name
                        email
                    }
                    state {
                        id
                        name
                        type
                        color
                    }
                    labels {
                        nodes {
                            id
                            name
                            color
                        }
                    }
                    createdAt
                    updatedAt
                    archivedAt
                    startedAt
                    completedAt
                    canceledAt
                    dueDate
                    triagedAt
                    snoozedUntilAt
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
    """,
    
    "projects": """
        query GetProjects($first: Int, $after: String) {
            projects(first: $first, after: $after) {
                nodes {
                    id
                    name
                    description
                    slugId
                    priority
                    state
                    startDate
                    targetDate
                    completedAt
                    canceledAt
                    archivedAt
                    createdAt
                    updatedAt
                    color
                    icon
                    creator {
                        id
                        name
                    }
                    lead {
                        id
                        name
                    }
                    teams {
                        nodes {
                            id
                            name
                            key
                        }
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
    """,
    
    "project_labels": """
        query GetProjectLabels($first: Int, $after: String) {
            projects(first: $first, after: $after) {
                nodes {
                    id
                    labels {
                        nodes {
                            id
                            name
                            color
                        }
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
    """,
    
    "project_updates": """
        query GetProjectUpdates($first: Int, $after: String, $filter: ProjectUpdateFilter) {
            projectUpdates(first: $first, after: $after, filter: $filter) {
                nodes {
                    id
                    body
                    health
                    diff
                    createdAt
                    updatedAt
                    archivedAt
                    project {
                        id
                        name
                    }
                    user {
                        id
                        name
                        email
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
    """,
    
    "cycles": """
        query GetCycles($first: Int, $after: String) {
            cycles(first: $first, after: $after) {
                nodes {
                    id
                    name
                    description
                    number
                    startsAt
                    endsAt
                    completedAt
                    archivedAt
                    createdAt
                    updatedAt
                    team {
                        id
                        name
                        key
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
    """,
    
    "workflow_states": """
        query GetWorkflowStates($first: Int, $after: String) {
            workflowStates(first: $first, after: $after) {
                nodes {
                    id
                    name
                    description
                    type
                    color
                    archivedAt
                    createdAt
                    updatedAt
                    team {
                        id
                        name
                        key
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
    """,

    "initiatives": """
        query GetInitiatives($first: Int, $after: String) {
            initiatives(first: $first, after: $after) {
                nodes {
                    id
                    name
                    description
                    sortOrder
                    targetDate
                    completedAt
                    archivedAt
                    createdAt
                    updatedAt
                    organization {
                        id
                        name
                    }
                    projects {
                        nodes {
                            id
                            name
                        }
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
    """
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Schemas - Flattened Structure

# COMMAND ----------

# Define flattened schemas for Delta tables
SCHEMAS = {
    "teams": StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("key", StringType(), True),
        StructField("description", StringType(), True),
        StructField("color", StringType(), True),
        StructField("icon", StringType(), True),
        StructField("private", BooleanType(), True),
        StructField("archived_at", TimestampType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("organization_id", StringType(), True),
        StructField("organization_name", StringType(), True)
    ]),
    
    "users": StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("display_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("avatar_url", StringType(), True),
        StructField("active", BooleanType(), True),
        StructField("admin", BooleanType(), True),
        StructField("guest", BooleanType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("archived_at", TimestampType(), True),
        StructField("timezone", StringType(), True)
    ]),
    
    "issues": StructType([
        StructField("id", StringType(), False),
        StructField("identifier", StringType(), True),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("priority", IntegerType(), True),
        StructField("estimate", DoubleType(), True),
        StructField("branch_name", StringType(), True),
        StructField("customer_ticket_count", IntegerType(), True),
        StructField("cycle_id", StringType(), True),
        StructField("cycle_name", StringType(), True),
        StructField("project_id", StringType(), True),
        StructField("project_name", StringType(), True),
        StructField("team_id", StringType(), True),
        StructField("team_name", StringType(), True),
        StructField("team_key", StringType(), True),
        StructField("assignee_id", StringType(), True),
        StructField("assignee_name", StringType(), True),
        StructField("assignee_email", StringType(), True),
        StructField("creator_id", StringType(), True),
        StructField("creator_name", StringType(), True),
        StructField("creator_email", StringType(), True),
        StructField("state_id", StringType(), True),
        StructField("state_name", StringType(), True),
        StructField("state_type", StringType(), True),
        StructField("state_color", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("archived_at", TimestampType(), True),
        StructField("started_at", TimestampType(), True),
        StructField("completed_at", TimestampType(), True),
        StructField("canceled_at", TimestampType(), True),
        StructField("due_date", DateType(), True),
        StructField("triaged_at", TimestampType(), True),
        StructField("snoozed_until_at", TimestampType(), True)
    ]),

    "projects": StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("slug_id", StringType(), True),
        StructField("priority", IntegerType(), True),
        StructField("state", StringType(), True),
        StructField("start_date", DateType(), True),
        StructField("target_date", DateType(), True),
        StructField("completed_at", TimestampType(), True),
        StructField("canceled_at", TimestampType(), True),
        StructField("archived_at", TimestampType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("color", StringType(), True),
        StructField("icon", StringType(), True),
        StructField("creator_id", StringType(), True),
        StructField("creator_name", StringType(), True),
        StructField("lead_id", StringType(), True),
        StructField("lead_name", StringType(), True)
    ]),
    
    "project_updates": StructType([
        StructField("id", StringType(), False),
        StructField("body", StringType(), True),
        StructField("health", StringType(), True),
        StructField("diff", StringType(), True),
        StructField("project_id", StringType(), True),
        StructField("project_name", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("user_email", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("archived_at", TimestampType(), True)
    ]),
    
    "cycles": StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("number", IntegerType(), True),
        StructField("starts_at", TimestampType(), True),
        StructField("ends_at", TimestampType(), True),
        StructField("completed_at", TimestampType(), True),
        StructField("archived_at", TimestampType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("team_id", StringType(), True),
        StructField("team_name", StringType(), True),
        StructField("team_key", StringType(), True)
    ]),
    
    "workflow_states": StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("type", StringType(), True),
        StructField("color", StringType(), True),
        # StructField("position", FloatType(), True),
        StructField("archived_at", TimestampType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("team_id", StringType(), True),
        StructField("team_name", StringType(), True),
        StructField("team_key", StringType(), True)
    ]),
    
    "issue_labels": StructType([
        StructField("issue_id", StringType(), False),
        StructField("label_id", StringType(), False),
        StructField("label_name", StringType(), True),
        StructField("label_color", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ]),
    
    "project_teams": StructType([
        StructField("project_id", StringType(), False),
        StructField("team_id", StringType(), False),
        StructField("team_name", StringType(), True),
        StructField("team_key", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ]),
    
    "project_labels": StructType([
        StructField("project_id", StringType(), True),
        StructField("label_id", StringType(), True),
        StructField("label_name", StringType(), True),
        StructField("label_color", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ]),

    "initiatives": StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("sort_order", DoubleType(), True),
    StructField("target_date", DateType(), True),
    StructField("completed_at", TimestampType(), True),
    StructField("archived_at", TimestampType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("organization_id", StringType(), True),
    StructField("organization_name", StringType(), True)
    ]),

    "initiative_projects": StructType([
    StructField("initiative_id", StringType(), False),
    StructField("project_id", StringType(), False),
    StructField("project_name", StringType(), True),
    StructField("created_at", TimestampType(), True)
    ])
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation Functions

# COMMAND ----------

def snake_case(string: str) -> str:
    """Convert camelCase to snake_case"""
    import re
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', string)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def transform_keys(obj: Any) -> Any:
    """Recursively transform camelCase keys to snake_case"""
    if isinstance(obj, dict):
        return {snake_case(key): transform_keys(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [transform_keys(item) for item in obj]
    else:
        return obj

def flatten_nested_objects(obj: Any) -> Any:
    """Flatten nested objects into prefixed fields"""
    if isinstance(obj, dict):
        flattened = {}
        for key, value in obj.items():
            if isinstance(value, dict) and value is not None:
                # Flatten nested dict with prefix
                for nested_key, nested_value in value.items():
                    flattened[f"{key}_{nested_key}"] = nested_value
            elif isinstance(value, list):
                # Keep lists as-is for now, will handle separately
                flattened[key] = value
            else:
                flattened[key] = value
        return flattened
    else:
        return obj

def transform_timestamps(obj: Any) -> Any:
    """Convert ISO timestamp strings to datetime objects"""
    if isinstance(obj, dict):
        transformed = {}
        for key, value in obj.items():
            if isinstance(value, str) and ('_at' in key or key.endswith('_date')) and value:
                try:
                    # Parse ISO format timestamp
                    if key in ['due_date', 'start_date', 'target_date']:
                        # Handle date-only fields
                        transformed[key] = datetime.strptime(value, '%Y-%m-%d').date()
                    else:
                        # Handle timestamp fields
                        transformed[key] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                except ValueError:
                    transformed[key] = value
            else:
                transformed[key] = transform_timestamps(value)
        return transformed
    elif isinstance(obj, list):
        return [transform_timestamps(item) for item in obj]
    else:
        return obj
    

def extract_relationship_tables(data: List[Dict], entity_type: str) -> Dict[str, List[Dict]]:
    """Extract relationship data into separate tables"""
    relationship_tables = {}
    current_time = datetime.now(UTC)
    
    if entity_type == 'issues':
        # Extract issue labels
        issue_labels = []
        for record in data:
            issue_id = record.get('id')
            labels = record.get('labels', [])
            
            # Handle both original API format and snake_case converted format
            if isinstance(labels, dict) and 'nodes' in labels:
                labels = labels['nodes']
            elif isinstance(labels, list):
                # Already in correct format
                pass
            else:
                labels = []
            
            if isinstance(labels, list):
                for label in labels:
                    if isinstance(label, dict):
                        issue_labels.append({
                            'issue_id': issue_id,
                            'label_id': label.get('id'),
                            'label_name': label.get('name'),
                            'label_color': label.get('color'),
                            'created_at': current_time
                        })
        
        if issue_labels:
            relationship_tables['issue_labels'] = issue_labels
    
    elif entity_type == 'projects' or entity_type=='project_labels':
        # Extract project teams and labels
        project_teams = []
        project_labels = []
        
        for record in data:
            project_id = record.get('id')
            
            # Handle teams
            teams = record.get('teams', [])
            # Handle both original API format and snake_case converted format  
            if isinstance(teams, dict) and 'nodes' in teams:
                teams = teams['nodes']
            elif 'teams_nodes' in record:
                teams = record.get('teams_nodes', [])
            
            if isinstance(teams, list):
                for team in teams:
                    if isinstance(team, dict):
                        project_teams.append({
                            'project_id': project_id,
                            'team_id': team.get('id'),
                            'team_name': team.get('name'),
                            'team_key': team.get('key'),
                            'created_at': current_time
                        })
            
            # Handle labels - check both possible field names after snake_case conversion
            labels = record.get('labels', [])
            if isinstance(labels, dict) and 'nodes' in labels:
                labels = labels['nodes']
            elif 'labels_nodes' in record:
                labels = record.get('labels_nodes', [])
            
            if isinstance(labels, list):
                for label in labels:
                    if isinstance(label, dict):
                        project_labels.append({
                            'project_id': project_id,
                            'label_id': label.get('id'),
                            'label_name': label.get('name'),
                            'label_color': label.get('color'),
                            'created_at': current_time
                        })
        
        if project_teams:
            relationship_tables['project_teams'] = project_teams
        if project_labels:
            relationship_tables['project_labels'] = project_labels
    
    elif entity_type == 'initiatives':
        # Extract initiative projects
        initiative_projects = []
        
        for record in data:
            initiative_id = record.get('id')
            
            # Handle projects
            projects = record.get('projects', [])
            # Handle both original API format and snake_case converted format
            if isinstance(projects, dict) and 'nodes' in projects:
                projects = projects['nodes']
            elif 'projects_nodes' in record:
                projects = record.get('projects_nodes', [])
            
            if isinstance(projects, list):
                for project in projects:
                    if isinstance(project, dict):
                        initiative_projects.append({
                            'initiative_id': initiative_id,
                            'project_id': project.get('id'),
                            'project_name': project.get('name'),
                            'created_at': current_time
                        })
        
        if initiative_projects:
            relationship_tables['initiative_projects'] = initiative_projects
    
    return relationship_tables

def process_linear_data(raw_data: List[Dict], entity_type: str) -> Dict[str, List[Dict]]:
    """Process raw Linear data for Delta table format with flattening"""

    # Transform keys to snake_case
    transformed_data = [transform_keys(record) for record in raw_data]
    
    # Transform timestamps
    transformed_data = [transform_timestamps(record) for record in transformed_data]
    
    # Flatten nested objects
    flattened_data = [flatten_nested_objects(record) for record in transformed_data]
    
    # Remove array fields from main records (they'll be in separate tables)
    for record in flattened_data:
        if 'labels' in record:
            del record['labels']
        if 'teams' in record:
            del record['teams']
    
    # Extract relationship tables
    relationship_tables = extract_relationship_tables(transformed_data, entity_type)
    
    # Return both main data and relationship tables
    result = {entity_type: flattened_data}
    result.update(relationship_tables)
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Extraction and Loading Functions

# COMMAND ----------

def extract_and_load_entity(entity_type: str, incremental: bool = True, days_back: int = 7):
    """Extract data for a specific entity type and load to Unity Catalog Delta table(s)"""
    
    print(f"Processing {entity_type}...")
    
    # Set up variables for the query
    variables = {}
    
    # For incremental loads, add filter for recent updates
    if incremental and entity_type in ["issues", "project_updates"]:
        cutoff_date = datetime.now(UTC) - timedelta(days=days_back)
        if entity_type == "issues":
            variables["filter"] = {
                "updatedAt": {
                    "gte": cutoff_date.isoformat()
                }
            }
        elif entity_type == "project_updates":
            variables["filter"] = {
                "updatedAt": {
                    "gte": cutoff_date.isoformat()
                }
            }
    
    # Extract data from Linear API
    try:
        raw_data = linear_client.paginated_query(
            QUERIES[entity_type], 
            variables=variables,
            page_size=100
        )
        print(f"Extracted {len(raw_data)} {entity_type} records")
        
        if not raw_data:
            print(f"No data found for {entity_type}")
            return
            
    except Exception as e:
        print(f"Error extracting {entity_type}: {str(e)}")
        return
    
    # Transform data (returns dict with main table and relationship tables)
    processed_data_dict = process_linear_data(raw_data, entity_type)
    
    # Load each table
    for table_type, data in processed_data_dict.items():
        if not data:
            continue
            
        # Create DataFrame
        if table_type in SCHEMAS:
            df = spark.createDataFrame(data, schema=SCHEMAS[table_type])
        else:
            print(f"Warning: No schema defined for {table_type}")
            continue
        
        # Define table name using Unity Catalog format
        table_name = f"{FULL_SCHEMA_NAME}.{table_type}"
        
        # Determine merge key
        if table_type == "issue_labels":
            merge_key = ["issue_id","label_id"]
        elif table_type == "project_teams":
            merge_key = ["project_id","team_id"]
        elif table_type == "project_labels":
            merge_key = ["project_id","label_id"]
        elif table_type == "initiative_projects":
            merge_key = ["initiative_id","project_id"]
        else:
            merge_key = ["id"]
        
        # Load to Delta table
        print("saving to " + table_name)
        if incremental is True:
            write_data(source_dataframe=df, target_table=table_name, merge_keys=merge_key, overwrite=False)
        else:
            write_data(source_dataframe=df, target_table=table_name, overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Execution

# COMMAND ----------

def run_full_pipeline():
    """Run the complete ELT pipeline for all entities"""
    
    print("Starting Linear ELT Pipeline...")
    print(f"Target catalog.schema: {FULL_SCHEMA_NAME}")
    
    # Define extraction order (dependencies first)
    entity_order = ["users", "teams", "workflow_states", "projects", "issues", "project_updates", "project_labels", "initiatives"]

    for entity in entity_order:
        try:
            extract_and_load_entity(entity, incremental=False)  # Set to True for incremental loads
            print(f"✅ Successfully processed {entity}")
        except Exception as e:
            print(f"❌ Failed to process {entity}: {str(e)}")
        print("-" * 50)
    
    print("Pipeline execution completed!")
    

def run_incremental_pipeline(days_back: int = 7):
    """Run incremental pipeline (mainly for issues and project updates)"""
    
    print(f"Starting incremental pipeline for last {days_back} days...")
    
    # For incremental runs, focus on frequently changing entities
    incremental_entities = ["issues", "projects", "project_updates", "project_labels", "initiatives"]
    
    for entity in incremental_entities:
        try:
            extract_and_load_entity(entity, incremental=True, days_back=days_back)
            print(f"✅ Successfully processed {entity} incrementally")
        except Exception as e:
            print(f"❌ Failed to process {entity}: {str(e)}")
        print("-" * 30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Pipeline
# MAGIC
# MAGIC Run the pipeline below. Choose between full or incremental load:

# COMMAND ----------

# Uncomment one of the following lines to run the pipeline:

if load_type == "full":
    # Full pipeline (loads all data)
    run_full_pipeline()
elif load_type == "incremental":
    # Incremental pipeline (loads only recent changes)
    run_incremental_pipeline(days_back=7)
else:
    raise ValueError("Invalid load type. Please specify 'full' or 'incremental'.")

