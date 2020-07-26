import csv
import click
from db import DB

db_client = DB()


@click.group()
def cli():
    pass


@cli.command()
@click.option("--resetdb/--no-resetdb", default=False)
@click.argument("input_file")
@click.argument("table")
def load_movies(table, input_file, resetdb):
    if resetdb:
        db_client.reset()
    with open(input_file) as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in reader:
            partition_key = row["region"]
            if partition_key == "\\N":
                partition_key = "none"
            sort_key = f"{row['titleId']}_{row['ordering'].rjust(3, '0')}"
            value = row["title"]
            db_client.set_value(table, partition_key, sort_key, value)


@cli.command()
@click.argument("table")
@click.argument("partition_key")
@click.argument("sortkey_start")
@click.argument("sortkey_end")
def get_movies(sortkey_end, sortkey_start, partition_key, table):
    values = db_client.get_range(table, partition_key, sortkey_start, sortkey_end)
    for value, score in values:
        print(score, value)


@cli.command()
@click.option("--batch-size", default=100)
@click.option("--end", default=1000)
@click.option("--start", default=1)
@click.argument("table")
@click.argument("partition_key")
def benchmark(partition_key, table, start, end, batch_size):
    for i in range(start, end, batch_size):
        start = str(i).zfill(7)
        end = str(i + 100).zfill(7)
        sortkey_start = f"tt{start}"
        sortkey_end = f"tt{end}"
        values = db_client.get_range(table, partition_key, sortkey_start, sortkey_end)
        print(len(values), sortkey_start, sortkey_end)


if __name__ == "__main__":
    cli()
