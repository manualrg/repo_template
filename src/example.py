import pandas as pd

def print_hello_world():
    print("Hello world!")

def test_this_module():
    return True


def rename_columns(dataframe: pd.DataFrame):
    return dataframe.rename(columns={
        "x1": "x_1",
        "x2": "x_2",
        "x3": "y"}
                           )

def multiply(dataframe: pd.DataFrame, factor: float):
    return dataframe * factor
    
def example_pipeline(dataframe: pd.DataFrame, factor: float):
    data = dataframe.copy()
    return data.pipe(rename_columns).pipe(multiply, factor=factor)