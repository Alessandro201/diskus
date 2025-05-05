use std::error::Error;
use std::fs::File;
use std::io::Write;

use tempdir::TempDir;

use diskus::{FilesizeType, Walk};

#[test]
fn size_of_single_file() -> Result<(), Box<dyn Error>> {
    let tmp_dir = TempDir::new("diskus-tests")?;

    let file_path = tmp_dir.path().join("file-100-byte");
    File::create(&file_path)?.write_all(&[0u8; 100])?;

    let num_threads = 1;
    let root_directories = &[file_path];
    let walk = Walk::new(root_directories.to_vec(), num_threads, FilesizeType::ApparentSize);
    let (sizes_in_bytes, errors) = walk.run();
    let (_dir, size_in_bytes) = sizes_in_bytes.first().expect("Should not be empty");

    assert!(errors.is_empty());
    assert_eq!(*size_in_bytes, 100);

    Ok(())
}
