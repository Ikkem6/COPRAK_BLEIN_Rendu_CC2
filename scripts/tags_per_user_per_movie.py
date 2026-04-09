from mrjob.job import MRJob

class TagsParUserParFilm(MRJob):
    def mapper(self, _, line):
        try:
            parts = line.strip().split(',')
            if parts[0] == 'userId':
                return
            if len(parts) < 4:
                return
            userId  = parts[0]
            movieId = parts[1]
            yield (movieId, userId), 1
        except Exception:
            pass

    def reducer(self, movie_user, counts):
        movieId, userId = movie_user
        yield (movieId, userId), sum(counts)

if __name__ == '__main__':
    TagsParUserParFilm.run()