using Dapper.Contrib.Extensions;
using FellowOakDicom;
using FellowOakDicom.Serialization;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using System.Data;
using System.Data.SqlClient;

namespace DicomIngestDemo {
    /// <summary>
    /// Dead simple demo program, with lots of bad practices.
    /// Keeping the data inserts small and unoptimized, to be as close to apples-to-apples comparison as possible
    /// Intentionally not multithreading or doing multiple inserts at a time.
    /// </summary>
    internal class Program {
        internal const string ingestInboxPath = @"C:\DICOM\IngestDemo\Inbox";
        internal const string ingestSourcePath = @"C:\DICOM\IngestDemo\Source";
        internal const string ingestErrorPath = @"C:\DICOM\IngestDemo\Error";
        internal const string ingestArchivePath = @"C:\DICOM\IngestDemo\Archive";

        internal const string sqlConnString = @"Data Source=10.0.0.12;Initial Catalog=DicomIngestDemo;User ID=DicomIngestDemo;Password=DicomIngestDemo;";
       
        internal const string mdbConnString = @"mongodb://10.0.0.12:27017";
        internal const string mdbName = "dicomIngestDemo";
        internal const string mDicomFileName = "dicomFile";
        internal const string mIngestHistoryName = "ingestHistory";

        static void Main() {
            using var watcher = new FileSystemWatcher(ingestInboxPath);

            watcher.NotifyFilter = NotifyFilters.Attributes;

            watcher.Changed += OnChanged;
            watcher.Deleted += OnDeleted;
            watcher.Error += OnError;

            watcher.Filter = "*.*";
            watcher.IncludeSubdirectories = false;
            watcher.EnableRaisingEvents = true;
            watcher.InternalBufferSize = 64 * 1024;

            // copy everything from source to inbox
            CopySourceToInbox(ingestSourcePath);

            Console.WriteLine("Press enter to exit.");
            Console.ReadLine();
        }

        private static void CopySourceToInbox(string sourceDirectory) {
            var sourceDir = new DirectoryInfo(sourceDirectory);
            var sourceDirs = sourceDir.GetDirectories();
            foreach (var file in sourceDir.GetFiles()) {
                var targetFilePath = Path.Combine(ingestInboxPath, $"{Guid.NewGuid()}.dcm");
                file.CopyTo(targetFilePath);
                File.SetAttributes(targetFilePath, FileAttributes.ReadOnly);
            }

            // wait to process files
            Thread.Sleep(sourceDir.GetFiles().Count() * 150);

            // recursive call
            foreach (var subDir in sourceDirs) {
                CopySourceToInbox(subDir.FullName);
            }
        }

        private static void OnDeleted(object sender, FileSystemEventArgs e) {
            // do nothing
        }

        private static void OnChanged(object sender, FileSystemEventArgs e) {
            // only proceed if the change was setting the file to readonly 
            var filePath = e.FullPath;
            var fileInfo = new FileInfo(filePath);
            if (!fileInfo.IsReadOnly)
                return;

            DateTime startTime;
            DateTime endTime;
            TimeSpan timeDiff;
            double tdMilliseconds;

            string dicomJson;
            Models.DicomFile dicomForImport;
            Models.IngestHistory historyForImport;

            try {
                // first open the DICOM file and parse the contents
                var dicomFile = DicomFile.Open(filePath);
                dicomJson = DicomJson.ConvertDicomToJson(dicomFile.Dataset);
                dicomForImport = new Models.DicomFile {
                    SopClassUid = dicomFile.Dataset.GetString(DicomTag.SOPClassUID),
                    SopInstanceUid = dicomFile.Dataset.GetString(DicomTag.SOPInstanceUID),
                    PatientId = dicomFile.Dataset.GetString(DicomTag.PatientID),
                    StudyInstanceUid = dicomFile.Dataset.GetString(DicomTag.StudyInstanceUID),
                    SeriesInstanceUid = dicomFile.Dataset.GetString(DicomTag.SeriesInstanceUID),
                    DicomJson = dicomJson
                };

                // next insert object and history into SQL
                using (IDbConnection db = new SqlConnection(sqlConnString)) {
                    startTime = DateTime.Now;

                    db.Insert(dicomForImport);

                    endTime = DateTime.Now;
                    timeDiff = endTime - startTime;
                    tdMilliseconds = timeDiff.TotalMilliseconds;

                    historyForImport = new Models.IngestHistory {
                        Filename = filePath,
                        StartTime = startTime,
                        EndTime = endTime,
                        ElapsedTimeMilliseconds = Convert.ToInt32(tdMilliseconds)
                    };

                    db.Insert(historyForImport);
                }

                // finally insert object and history into MongoDB
                // TODO remember to add index on SOP class, SOP instance, Patient, Study, Series ID's after inserting some data
                var mdbClient = new MongoClient(mdbConnString);
                var mdb = mdbClient.GetDatabase(mdbName);
                var mDicomFile = mdb.GetCollection<BsonDocument>(mDicomFileName);
                var mIngestHistory = mdb.GetCollection<BsonDocument>(mIngestHistoryName);

                //var dicomBson = dicomForImport.ToBsonDocument();
                var dicomBson = BsonSerializer.Deserialize<BsonDocument>(dicomJson);

                startTime = DateTime.Now;

                mDicomFile.InsertOne(dicomBson);

                endTime = DateTime.Now;
                timeDiff = endTime - startTime;
                tdMilliseconds = timeDiff.TotalMilliseconds;

                historyForImport.StartTime = startTime;
                historyForImport.EndTime = endTime;
                historyForImport.ElapsedTimeMilliseconds = Convert.ToInt32(tdMilliseconds);
                var historyBson = historyForImport.ToBsonDocument();

                mIngestHistory.InsertOne(historyBson);
            }
            catch (Exception ex) {
                PrintException(ex);
                if (File.Exists(filePath))
                    File.Move(filePath, Path.Combine(ingestErrorPath, Path.GetFileName(filePath)), true);
            }
            finally {
                if (File.Exists(filePath)) {
                    var archivePath = Path.Combine(ingestArchivePath, Path.GetFileName(filePath));
                    File.Move(filePath, archivePath, true);
                    File.SetAttributes(archivePath, FileAttributes.Normal);
                    File.Delete(archivePath);
                }

                Console.WriteLine($"Processed {filePath}");
            }
        }

        private static void OnError(object sender, ErrorEventArgs e) =>
            PrintException(e.GetException());

        private static void PrintException(Exception? ex) {
            if (ex != null) {
                Console.WriteLine($"Message: {ex.Message}");
                Console.WriteLine("Stacktrace:");
                Console.WriteLine(ex.StackTrace);
                Console.WriteLine();
                PrintException(ex.InnerException);
            }
        }
    }
}
