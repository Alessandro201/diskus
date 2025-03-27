use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;

use crossbeam_channel as channel;

use colored::Colorize;
use humansize::file_size_opts::FileSizeOpts;
use humansize::FileSize;
use rayon::{self, prelude::*};

use crate::filesize::FilesizeType;
use crate::unique_id::{generate_unique_id, UniqueID};

fn print_result<P: AsRef<Path>>(
    path: P,
    size: Option<u64>,
    error: Option<Error>,
    size_format: &FileSizeOpts,
) {
    if let Some(err) = error {
        match err {
            Error::NoMetadataForPath(path) => {
                eprintln!(
                    "{} could not retrieve metadata for path '{}'",
                    "diskus:".red().bold(),
                    path.to_string_lossy()
                );
            }
            Error::CouldNotReadDir(path) => {
                eprintln!(
                    "{} could not read contents of directory '{}'",
                    "diskus:".red().bold(),
                    path.to_string_lossy()
                );
            }
        }
    }

    if let Some(size) = size {
        if atty::is(atty::Stream::Stdout) {
            println!(
                "{: >10}    {}",
                size.file_size(size_format).unwrap(),
                path.as_ref().to_str().unwrap()
            );
        } else {
            println!("{}\t{:?}", size, path.as_ref().to_str().unwrap());
        }
    }
}

pub enum Error {
    NoMetadataForPath(PathBuf),
    CouldNotReadDir(PathBuf),
}

enum Message {
    SizeEntry(Option<UniqueID>, PathBuf, u64),
    FinishedEntry(PathBuf),
    Error { error: Error },
}

fn root_walk(tx: channel::Sender<Message>, entries: Vec<PathBuf>, filesize_type: FilesizeType) {
    entries.into_par_iter().for_each_with(tx, |tx_ref, entry| {
        walk(
            tx_ref.clone(),
            &[entry.clone()],
            entry.clone(),
            0,
            filesize_type,
        );
    })
}

fn walk(
    tx: channel::Sender<Message>,
    entries: &[PathBuf],
    root: PathBuf,
    depth: u64,
    filesize_type: FilesizeType,
) {
    entries
        .into_par_iter()
        .for_each_with(tx.clone(), |tx_ref, entry| {
            if let Ok(metadata) = entry.symlink_metadata() {
                let unique_id = generate_unique_id(&metadata);

                let size = filesize_type.size(&metadata);

                tx_ref
                    .send(Message::SizeEntry(unique_id, root.clone(), size))
                    .unwrap();

                if metadata.is_dir() {
                    let mut children = vec![];
                    match fs::read_dir(entry) {
                        Ok(child_entries) => {
                            for child_entry in child_entries.flatten() {
                                children.push(child_entry.path());
                            }
                        }
                        Err(_) => {
                            tx_ref
                                .send(Message::Error {
                                    error: Error::CouldNotReadDir(entry.clone()),
                                })
                                .unwrap();
                        }
                    }

                    walk(
                        tx_ref.clone(),
                        &children[..],
                        root.clone(),
                        depth + 1,
                        filesize_type,
                    );
                };
            } else {
                tx_ref
                    .send(Message::Error {
                        error: Error::NoMetadataForPath(entry.clone()),
                    })
                    .unwrap();
            };
        });
    if depth == 0 {
        tx.send(Message::FinishedEntry(root.clone())).unwrap();
    }
}

pub struct Walk {
    root_directories: Vec<PathBuf>,
    num_threads: usize,
    filesize_type: FilesizeType,
}

impl Walk {
    pub fn new(
        root_directories: Vec<PathBuf>,
        num_threads: usize,
        filesize_type: FilesizeType,
    ) -> Walk {
        Walk {
            root_directories,
            num_threads,
            filesize_type,
        }
    }

    // pub fn run(&self) -> (Vec<(PathBuf, u64)>, Vec<Error>) {
    pub fn run(&self) -> (Vec<(PathBuf, u64)>, Vec<Error>) {
        let (tx, rx) = channel::unbounded();

        let receiver_thread = thread::spawn(move || {
            let mut ids = HashSet::new();
            let mut roots_sizes = HashMap::new();
            let mut error_messages: Vec<Error> = Vec::new();
            for msg in rx {
                match msg {
                    Message::SizeEntry(unique_id, root, size) => {
                        if let Some(unique_id) = unique_id {
                            // Only count this entry if the ID has not been seen
                            if ids.insert(unique_id) {
                                roots_sizes
                                    .entry(root)
                                    .and_modify(|tot| *tot += size)
                                    .or_insert(size);
                            }
                        } else {
                            roots_sizes
                                .entry(root)
                                .and_modify(|tot| *tot += size)
                                .or_insert(size);
                        }
                    }
                    Message::Error { error } => {
                        error_messages.push(error);
                    }
                    Message::FinishedEntry(_) => {}
                }
            }
            let mut sizes: Vec<(PathBuf, u64)> = Vec::new();
            for (root, size) in roots_sizes {
                sizes.push((root.clone(), size));
            }
            (sizes, error_messages)
        });

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.num_threads)
            .build()
            .unwrap();
        pool.install(|| root_walk(tx, self.root_directories.clone(), self.filesize_type));

        receiver_thread.join().unwrap()
    }

    pub fn run_and_print(&self, size_format: FileSizeOpts, total: bool, verbose: bool) {
        let (tx, rx) = channel::unbounded();

        let receiver_thread = thread::spawn(move || {
            let mut ids = HashSet::new();
            let mut sizes = HashMap::new();
            let mut tainted_results = false;
            for msg in rx {
                match msg {
                    Message::SizeEntry(unique_id, root, size) => {
                        if let Some(unique_id) = unique_id {
                            // Only count this entry if the ID has not been seen
                            if ids.insert(unique_id) {
                                sizes
                                    .entry(root)
                                    .and_modify(|tot| *tot += size)
                                    .or_insert(size);
                            }
                        } else {
                            sizes
                                .entry(root)
                                .and_modify(|tot| *tot += size)
                                .or_insert(size);
                        }
                    }
                    Message::Error { error } => {
                        if verbose {
                            print_result("", None, Some(error), &size_format)
                        } else {
                            tainted_results = true;
                        }
                    }
                    Message::FinishedEntry(path) => print_result(
                        &path,
                        sizes.get(&path).map(|s| s.to_owned()),
                        None,
                        &size_format,
                    ),
                }
            }

            if tainted_results {
                eprintln!(
                    "{} the results may be tainted. Re-run with -v/--verbose to print all errors.",
                    "[diskus warning]".red().bold()
                );
            }

            if total {
                let total_size = sizes.values().sum();
                println!("\n{}", "Total:".cyan().bold());
                print_result("", Some(total_size), None, &size_format);
            }
        });

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.num_threads)
            .build()
            .unwrap();
        pool.install(|| root_walk(tx, self.root_directories.clone(), self.filesize_type));

        receiver_thread.join().unwrap()
    }
}
