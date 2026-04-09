from mrjob.job import MRJob

class FrequenceTag(MRJob):
    def mapper(self, _, line):
        try:
            parts = line.strip().split(',')
            if parts[0] == 'userId':
                return
            if len(parts) < 4:
                return
            tag = ','.join(parts[2:-1]).strip('"').lower()
            yield tag, 1
        except Exception:
            pass

    def reducer(self, tag, counts):
        yield tag, sum(counts)

if __name__ == '__main__':
    FrequenceTag.run()