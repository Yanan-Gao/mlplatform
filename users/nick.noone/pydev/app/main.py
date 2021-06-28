from app.src.tf_utils import DataSource


def main():
    print(f"Starting...")
    src = DataSource()
    df = src.get_data()
    print(df)

main()