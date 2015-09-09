import uuid
from mrjob.job import MRJob


class TranscoderJob(MRJob):

    def configure_options(self):
        super(TranscoderJob, self).configure_options()
        self.add_passthrough_option(
            '--files-root', type='str', default='.',
            help='Path where all files are located')

    def load_options(self, args):
        super(TranscoderJob, self).load_options(args)
        self.files_root = self.options.files_root

    def _fetch_file(self, filename):
        return open(self.files_root + '/' + filename, 'r')

    def _transcode(self, chunk):
        return ''.join(str(ord(ch)) for ch in chunk)

    def _transfer_transcoded_file(self, transcoded_chunk):
        filename = uuid.uuid4().hex
        f = open(self.files_root + '/' + filename, 'w')
        f.write(transcoded_chunk + '\n')
        f.close()

        return filename

    def mapper(self, _, filename):
        chunk_file = self._fetch_file(filename)
        key, chunk = tuple(chunk_file.read().split())
        transcoded_chunk = self._transcode(chunk)
        transcoded_filename = self._transfer_transcoded_file(transcoded_chunk)
        yield key, transcoded_filename

    def reducer(self, key, values):
        result = ''
        for f in values:
            result += (self._fetch_file(f).read() + '\n')

        yield key, result


if __name__ == '__main__':
    TranscoderJob.run()
