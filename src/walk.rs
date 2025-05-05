use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::thread;
use std::{fs, io};

use crossbeam_channel as channel;

use colored::Colorize;
use humansize::file_size_opts::FileSizeOpts;
use humansize::FileSize;
use rayon::{self, prelude::*};

use crate::filesize::FilesizeType;
use crate::unique_id::{generate_unique_id, UniqueID};

fn safe_write(s: String) {
    match io::stdout().write_all(s.as_bytes()) {
        Ok(_) => {}
        Err(ref err) if err.kind() == io::ErrorKind::BrokenPipe => exit(0),
        Err(ref err) => {
            eprintln!("Unexpected err: {:?}", err);
            exit(1)
        }
    }
}

fn print_result<P: AsRef<Path>>(
    path: P,
    size: Option<u64>,
    error: Option<Error>,
    size_format: Option<&FileSizeOpts>,
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
        // if atty::is(atty::Stream::Stdout) {
        //     println!(
        //         "{: >10}    {}",
        //         size.file_size(size_format).unwrap(),
        //         path.as_ref().to_str().unwrap()
        //     );
        // } else {
        //     println!("{}\t{:?}", size, path.as_ref().to_str().unwrap());
        // }

        if let Some(size_format) = size_format {
            safe_write(format!(
                "{: >10}    {}\n",
                size.file_size(size_format).unwrap(),
                path.as_ref().to_str().unwrap()
            ));
            // println!(
            //     "{: >10}    {}",
            //     size.file_size(size_format).unwrap(),
            //     path.as_ref().to_str().unwrap()
            // );
        } else {
            safe_write(format!("{}\t{}\n", size, path.as_ref().to_str().unwrap()));
            // println!("{}\t{:?}", size, path.as_ref().to_str().unwrap());
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

    pub fn run(&self) -> (Vec<(PathBuf, u64)>, Vec<Error>) {
        let (tx, rx) = channel::unbounded();

        let receiver_thread = thread::spawn(move || {
            let mut ids = HashSet::new();
            let mut sizes = HashMap::new();
            let mut error_messages = vec![];
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
                        error_messages.push(error);
                    }
                    Message::FinishedEntry(_path) => {}
                }
            }
            let mut sizes_vec = vec![];
            for (path, size) in sizes.into_iter() {
                sizes_vec.push((path, size));
            }
            (sizes_vec, error_messages)
        });

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.num_threads)
            .build()
            .unwrap();
        pool.install(|| root_walk(tx, self.root_directories.clone(), self.filesize_type));

        receiver_thread.join().unwrap()
    }

    pub fn run_and_print_sorted(
        &self,
        size_format: Option<FileSizeOpts>,
        print_total: bool,
        verbose: bool,
    ) {
        let (mut sizes, error_messages) = self.run();
        sizes.sort_by(|(_p1, s1), (_p2, s2)| s1.cmp(s2));

        if verbose {
            for err in error_messages {
                print_result("", None, Some(err), None);
            }
        } else if !error_messages.is_empty() {
            eprintln!(
                "{} the results may be tainted. Re-run with -v/--verbose to print all errors.",
                "[diskus warning]".red().bold()
            );
        }

        let mut total_size = 0;
        for (path, size) in sizes {
            total_size += size;
            print_result(path, Some(size), None, size_format.as_ref());
        }

        if print_total {
            println!("\n{}", "Total:".cyan().bold());
            print_result("", Some(total_size), None, size_format.as_ref());
        }
    }

    pub fn run_and_print(&self, size_format: Option<FileSizeOpts>, total: bool, verbose: bool) {
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
                            print_result("", None, Some(error), size_format.as_ref())
                        } else {
                            tainted_results = true;
                        }
                    }
                    Message::FinishedEntry(path) => print_result(
                        &path,
                        sizes.get(&path).map(|s| s.to_owned()),
                        None,
                        size_format.as_ref(),
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
                print_result("", Some(total_size), None, size_format.as_ref());
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
