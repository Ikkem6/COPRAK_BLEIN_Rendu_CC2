from mrjob.job import MRJob

class TagsPerMovie(MRJob):

    def mapper(self, _, line):
        try:
            parts = line.split(',')
            if parts[0] == 'userId':  
                return
            userId, movieId, tag, timestamp = parts[0], parts[1], parts[2], parts[3]
            yield movieId, 1
        except Exception:
            pass

    def reducer(self, movieId, counts):
        yield movieId, sum(counts)

if __name__ == '__main__':
    TagsPerMovie.run()