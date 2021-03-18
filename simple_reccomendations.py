import PostgresDAO
import random


def construct_insert_query(table_name: str, var_names: list[str]) -> str: #TODO: Don't copy/paste from mongo_to_pg
    """Constructs an SQL insert query, formatted to use %s in place of actual values to allow PsycoPG2 to properly format them.

    Args:
        table_name: the name of the table to insert to insert into.
        var_names: a list containing all the attributes to insert.

    Returns:
        A properly formatted SQL query as string.
    """
    q = f"INSERT INTO {table_name} ("
    q += var_names[0]
    for var in var_names[1:]:
        q += ", " + var
    q += f") VALUES (%s{(len(var_names) - 1) * ', %s'});"
    return q


def create_rcmd_table(db: PostgresDAO.PostgreSQLdb, table_name: str, unique_attributes: list[tuple[str, str]]):
    """Create a table in a PostgreSQL database for the purpose of filling it with reccomendations.
    Create it according to this format:
    any number of columns that are primary keys to store the unique attributes by.
    4 foreign key columns to fill with product_ids to recommend in association with the unique attributes.

    Args:
        db: The PostgreSQL db to put the table in.
        table_name: the name the newly created table should have.
        unique_attributes: Any amount of tuples, 1 for each unique attribute to recommend by. Each must contain:
            0: The name the attribute should get in the newly created table.
            1: The PostgreSQL datatype (preferebly capitalized) the new attribute should have.

    The 4 recommendation columns will be called (rcmd_1 ... rcmd_4)."""
    db.query(f"DROP TABLE IF EXISTS {table_name};", commit_changes=True)
    query = f"CREATE TABLE {table_name}(\n"
    for attribute_name, attribute_type in unique_attributes:
        query += f"{attribute_name} {attribute_type},\n"
    query += f"""rcmd_1 VARCHAR,
rcmd_2 VARCHAR,
rcmd_3 VARCHAR,
rcmd_4 VARCHAR,
PRIMARY KEY({unique_attributes[0][0]}"""
    for atttribute_name, attribute_type in unique_attributes[1:]:
        query += f", {atttribute_name}"
    query +="""),
FOREIGN KEY(rcmd_1) REFERENCES Products(id),
FOREIGN KEY(rcmd_2) REFERENCES Products(id),
FOREIGN KEY(rcmd_3) REFERENCES Products(id),
FOREIGN KEY(rcmd_4) REFERENCES Products(id)
);"""
    db.query(query, commit_changes=True)


def group_data_by_unique_identifiers(dataset: list[tuple], index_list: list[int]) -> dict:
    """Recursive function to group a dataset as formatted by Psycopg2 SELECT query into
    all possible permutation of certain unique attributes from the query
    in a dictionairy-in-dictionairy structure for content-filtering.

    For instance, when given the result of a 'SELECT persona_id, city, age FROM persons;' query in a hypothetical database
    as dataset parameter, with the indexes [1, 2] as index_list parameter.
    It will create a dictionairy with every city in existance as keys.
    Which each then contain every age that exists in that city as keys.
    Which in turn, contains a list containing tuples that contain (persona_id, city, age)
    for every person in that city/age combination.

    NOTE: More relevant attributes should be first in index_list, as they will be given priority when
    this datastructure is used in later functions if a certain attribute permutation doesn't contain enough
    products to recommend.

    Args:
        dataset: dataset as formatted by Psycopg2 SELECT query
        index_list: list of the index of every attribute in the SELECT query that the data should be grouped by.
        
    Returns:
        #(index_list) levels of dictionairy in dictionairy,
        where the keys of a given dictionairy of level n are the permutations of the attribute at index index_list[n]
        (that exist in the previous attribute if n != 0)"""
    unique_identifiers = dict()
    for entry in dataset:
        identifier = entry[index_list[0]]
        if identifier in unique_identifiers:
            unique_identifiers[identifier].append(entry)
        else:
            unique_identifiers[identifier] = [entry]
    if len(index_list) > 1:
        for k, v in unique_identifiers.items():
            unique_identifiers[k] = group_data_by_unique_identifiers(v, index_list[1:])
    return unique_identifiers


def content_filter_recommendations_from_grouped_data(data: dict or list, original_dataset: list[tuple], current_level: int = 0, recommendation_amount: int = 4) -> tuple[list, list]:
    """Recursively generate a given amount of recommendations from data as formatted by group_data_by_unique_identifiers().
    Refer to that function's documentation for more information of datastructure.
    When an attribute permutation has enough products to recommend (4 by default), recommends products entirely in it.
    When the attribute permutation doesn't, will attempt to fill the rest of the recommendation with products products,
    dropping attribute requirements starting at the end one by one, until enough products to recommend are found.
    If this still doesn't result in enough products to recommend, will fill the rest of the recommendation with random products.

    Args:
        data: the dict_in_dict stucture as provided by group_data_by_unique_identifiers()
        original_dataset: the original PostgreSQL query result, to fetch random products from.
        current_level: current level of recursion, should always be left on 0 on initial method call.
        recommendation_amount: the amount of products that should be recommended per attribute permutation.

    Returns:
        list containing lists for every recommendation. Every nested list contains:
            A tuple for every product in the recommendation that contain:
                All attributes for that product, formatted exactly as they were in the Psycopg2 query result."""
    if isinstance(data, dict): #TODO: Add more comments
        dataset, incomplete_indexes, current_index_in_dataset = [], [], 0
        for k, v in data.items():#unpack every v one level deeper
            result_from_one_layer_deeper, incomplete_data = content_filter_recommendations_from_grouped_data(v, original_dataset, current_level + 1)
            incomplete_indexes += [x + current_index_in_dataset for x in incomplete_data]
            for item in result_from_one_layer_deeper:
                dataset.append(item)
                current_index_in_dataset += 1
        able_to_complete_datapoints = True
        for index in incomplete_indexes: #try to finish all incomplete datapoints
            if len(dataset) - len(incomplete_indexes) >= recommendation_amount: #Try to use incomplete_indexes to finish the incomplete datapoints
                for i in range(len(dataset[index]), recommendation_amount):
                    random_datapoint = random.choice(random.choice(dataset))
                    dataset[index].append(random_datapoint)
            elif current_level == 0: #Random additions from the original dataset
                for i in range(len(dataset[index]), recommendation_amount):
                    dataset[index].append(random.choice(original_dataset))
            else:
                able_to_complete_datapoints = False
                break
        if able_to_complete_datapoints:
            incomplete_indexes = []
        return dataset, incomplete_indexes #change to index
    else:
        if len(data) < recommendation_amount:
            return [random.sample(data, min(recommendation_amount, len(data)))], [0]
        else:
            return [random.sample(data, recommendation_amount)], []


def content_filter_result_to_useful_SQL_dataset(data: list[list[tuple]]) -> list[tuple]:
    """Generate a dataset that can be used for PostgresDAO.PostgreSQLdb.many_update_queries()
    out of the recommendation dataset as gained by content_filter_recommendations_from_grouped_data().

    Args:
        data: The recommendation dataset as gained by content_filter_recommendations_from_grouped_data()
            -Assumes the product ID is the first attribute, and the attributes following that are the attributes to recommend based on.

    Returns:
        Dataset that can be used for PostgresDAO.PostgreSQLdb.many_update_queries()"""
    for recommendation in data[0]:
        result_data = []
        print(f"Recommendation: {recommendation}")
        for unique_attribute in recommendation[0][1:]:
            result_data.append(unique_attribute)
        for product in recommendation:
            result_data.append(product[0])


products = PostgresDAO.db.query("SELECT id, category, brand FROM products;", expect_return=True)
grouped = group_data_by_unique_identifiers(products, [1, 2])
recommendations = content_filter_recommendations_from_grouped_data(grouped, products)
dataset = content_filter_result_to_useful_SQL_dataset(recommendations)

create_rcmd_table(PostgresDAO.db, "Content_filtered", [("Category", "VARCHAR"), ("Brand", "VARCHAR")])
query = construct_insert_query("Content_filtered", ["Category", "Brand"])

PostgresDAO.db.many_update_queries(query, dataset)