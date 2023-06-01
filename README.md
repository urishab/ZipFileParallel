# ZipFileParallel
python zipfile allows reading in multi-threading but not writing. This class allows writing in parallel
This code adds a new class, ZipFileParallel, that allows writestr function to work in parallel when not writing to the file.
That is the compression part (CPU heavy) can happen in multiple threads and the writing to disk (IO heavy) happens in series.

This way you can use multithreading tool like ThreadPoolExecutor to concurrently compress and write files to a zipfile
For example:
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []

        for img in enumerate(images):
            fname = f'images/{idx}.raw'
            futures.append(executor.submit(file.writestr, fname ,img.tobytes()))

        concurrent.futures.wait(futures)
