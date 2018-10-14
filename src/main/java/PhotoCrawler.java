import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import model.Photo;
import model.PhotoSize;
import util.PhotoDownloader;
import util.PhotoProcessor;
import util.PhotoSerializer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PhotoCrawler {

    private static final Logger log = Logger.getLogger(PhotoCrawler.class.getName());

    private final PhotoDownloader photoDownloader;

    private final PhotoSerializer photoSerializer;

    private final PhotoProcessor photoProcessor;

    public PhotoCrawler() throws IOException {
        this.photoDownloader = new PhotoDownloader();
        this.photoSerializer = new PhotoSerializer("./photos");
        this.photoProcessor = new PhotoProcessor();
    }

    public void resetLibrary() throws IOException {
        photoSerializer.deleteLibraryContents();
    }

    public void downloadPhotoExamples() {
        try {
//            Observable<Photo> downloadedExamples =
            photoDownloader
                    .getPhotoExamples()
                    .subscribe(photoSerializer::savePhoto);
//            for (Photo photo : downloadedExamples) {
//                photoSerializer.savePhoto(photo);
//            }
        } catch (IOException e) {
            log.log(Level.SEVERE, "Downloading photo examples error", e);
        }
    }

    public void downloadPhotosForQuery(String query) throws IOException {
        photoDownloader
                .searchForPhotos(query)
                .compose(this::processPhotos)
                .subscribe(photoSerializer::savePhoto, System.out::println);
    }

    public void downloadPhotosForMultipleQueries(List<String> queries) throws IOException {
        photoDownloader
                .searchForPhotos(queries)
//                .compose(this::processPhotos)
                .compose(this::processGroupedPhotos)
                .subscribe(photoSerializer::savePhoto, System.out::println);
    }

    public Observable<Photo> processPhotos(Observable<Photo> photo) {
        return photo
                .filter(photoProcessor::isPhotoValid)
                .map(photoProcessor::convertToMiniature)
                .subscribeOn(Schedulers.io());
    }

    public Observable<Photo> processGroupedPhotos(Observable<Photo> photo) {
        return photo
                .filter(photoProcessor::isPhotoValid)
                .groupBy(PhotoSize::resolve)
                .flatMap(groupedObservable -> {
                    if (groupedObservable.getKey() == PhotoSize.MEDIUM) {
                        return groupedObservable
                                .buffer(5, 5, TimeUnit.SECONDS)
                                .flatMap(Observable::fromIterable);
                    } else {
                        return groupedObservable
                                .observeOn(Schedulers.computation())
                                .map(photoProcessor::convertToMiniature);
                    }
                });
    }
}
