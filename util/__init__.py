import pandas as pd
from datetime import date
import local_db

# create active student list from 2-year rolling window
def active_students():
    """
    returns DataFrame of active student IDs

    Active Students are those that have been enrolled in last two years.
    """

    connection = local_db.connection()

    today = date.today()
    two_years_ago = today.year - 2
    sql_str = (
        "SELECT PEOPLE_CODE_ID FROM ACADEMIC WHERE "
        + f"ACADEMIC_YEAR > '{two_years_ago}' "
        + "AND PRIMARY_FLAG = 'Y' "
        + "AND CURRICULUM NOT IN ('ADVST') "
        + "AND GRADUATED NOT IN ('G') "
    )
    return ( pd.read_sql_query(sql_str, connection)
               .drop_duplicates(["PEOPLE_CODE_ID"])
    )


# create user list of PEOPLE_CODE_ID's with college email_addresses
def with_email_address():
    """
    returns DataFrame of PEOPLE_CODE_ID's with non-NULL college email_addresses

    """

    connection = local_db.connection()

    sql_str = (
        "SELECT PeopleOrgCodeId FROM EmailAddress WHERE "
        + "IsActive = 1 "
        + "AND (EmailType='HOME' OR EmailType='MLBX') "
        + "AND Email LIKE '%@%' "
    )

    return ( pd.read_sql_query(sql_str, connection)
             .drop_duplicates(["PeopleOrgCodeId"])
             .rename(columns={"PeopleOrgCodeId": "PEOPLE_CODE_ID"})
    )


def apply_active(in_df):
    """
    returns copy of in_df with only records for active students

    in_df is an input DataFrame, must have PEOPLE_CODE_ID field
    """

    # return records for active students
    return pd.merge(in_df, active_students(), how="inner", on="PEOPLE_CODE_ID")


def apply_active_with_email_address(in_df):
    """
    returns copy of in_df with only records for active students with email_address

    in_df is an input DataFrame, must have PEOPLE_CODE_ID field
    """

    # return records for active students with email_address
    return pd.merge(apply_active(in_df=in_df), with_email_address(), how="inner", on="PEOPLE_CODE_ID")


# find the latest year_term
def latest_year_term(df):
    """
    Return df with most recent records based on ACADEMIC_YEAR and ACADEMIC_TERM
    """
    df = df.copy()
    df = df[(df["ACADEMIC_YEAR"].notnull()) & (df["ACADEMIC_YEAR"].str.isnumeric())]
    df["ACADEMIC_YEAR"] = pd.to_numeric(df["ACADEMIC_YEAR"], errors="coerce")
    df_seq = pd.DataFrame(
        [
            {"term": "Transfer", "seq": 0},
            {"term": "SPRING", "seq": 1},
            {"term": "SUMMER", "seq": 2},
            {"term": "FALL", "seq": 3},
        ]
    )
    df = pd.merge(df, df_seq, left_on="ACADEMIC_TERM", right_on="term", how="left")
    df["term_seq"] = df["ACADEMIC_YEAR"] * 100 + df["seq"]

    #d = df.reset_index().groupby(["PEOPLE_CODE_ID"])["term_seq"].idxmax()

    df = df.loc[df.reset_index().groupby(["PEOPLE_CODE_ID"])["term_seq"].idxmax()]

    return df

