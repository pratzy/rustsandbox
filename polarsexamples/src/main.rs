use std::fs::File;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{Float64Array, StringArray};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;

const ARROW_FILE_PATH: &str = "./resources/data/out.arrow";

fn create_record_batch() -> RecordBatch {
    let col_0 = Arc::new(StringArray::from_iter([
        Some("AAPL"),
        Some("AMZN"),
        Some("GOOG"),
    ])) as _;
    let col_1 = Arc::new(Float64Array::from_iter([1.0, 2.2, 3.2])) as _;
    let col_2 = Arc::new(Float64Array::from_iter([1., 6.3, 4.])) as _;

    let batch =
        RecordBatch::try_from_iter([("col_0", col_0), ("col1", col_1), ("col_2", col_2)]).unwrap();

    println!("batch: {:?}", batch);
    println!("Schema: {:?}", batch.schema());

    batch
}

fn write_to_arrow(batch: &RecordBatch, fpath: &str) -> Result<()> {
    let f = File::create(fpath).expect("Could not create file");
    let mut writer = FileWriter::try_new(f, &batch.schema())?;
    writer.write(&batch).expect("Not written");
    writer.finish().expect("Could not flush");
    Ok(())
}

fn read_from_arrow_file(fpath: &str) -> Result<RecordBatch> {
    let f = File::open(fpath)?;
    let mut reader = FileReader::try_new(f, None)?;
    let batch = reader.next().unwrap().unwrap();
    println!("Read file at {fpath}");
    Ok(batch)
}

fn main() {
    let batch = create_record_batch();
    write_to_arrow(&batch, ARROW_FILE_PATH).expect("Error writing to file");
    let r = read_from_arrow_file(ARROW_FILE_PATH).expect("Error reading file");
    println!("Batch read: {:?}", r);
}
