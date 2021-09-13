package net.pedegie.stats.api.queue


import spock.lang.Specification
import spock.lang.Unroll

import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

class RecyclerTest extends Specification
{
    def setup()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    def cleanupSpec()
    {
        FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
    }

    @Unroll()
    def "should correctly name log files: #time resolves to #expectedDateAppendedToLogFile"()
    {
        given:
            ZonedDateTime now = ZonedDateTime.of(LocalDateTime.parse(time), ZoneId.of("UTC"))
            LogFileConfiguration logFileConfiguration = LogFileConfiguration
                    .builder()
                    .path(TestQueueUtil.PATH)
                    .fileCycleDuration(fileCycleDuration)
                    .fileCycleClock(Clock.fixed(now.toInstant(), ZoneId.of("UTC")))
                    .build()
            TestQueueUtil.createQueue(logFileConfiguration).close()
        expect:
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            Path expectedLogFileName = PathDateFormatter.appendDate(TestQueueUtil.PATH, ZonedDateTime.of(LocalDateTime.parse(expectedDateAppendedToLogFile), ZoneId.of("UTC")))
            logFile.toString() == expectedLogFileName.toString()
            FileUtils.cleanDirectory(TestQueueUtil.PATH.getParent())
        where: "within minutes"
            time                  | fileCycleDuration                  | expectedDateAppendedToLogFile
            // should narrow all to 15:00:00
            "2020-01-03T15:00:00" | Duration.of(1, ChronoUnit.MINUTES) | "2020-01-03T15:00:00"
            "2020-01-03T15:00:01" | Duration.of(1, ChronoUnit.MINUTES) | "2020-01-03T15:00:00"
            "2020-01-03T15:00:30" | Duration.of(1, ChronoUnit.MINUTES) | "2020-01-03T15:00:00"
            "2020-01-03T15:00:59" | Duration.of(1, ChronoUnit.MINUTES) | "2020-01-03T15:00:00"
            // and then step to 01:00
            "2020-01-03T15:01:00" | Duration.of(1, ChronoUnit.MINUTES) | "2020-01-03T15:01:00"
            // two minutes cycle narrow
            "2020-01-03T15:01:30" | Duration.of(2, ChronoUnit.MINUTES) | "2020-01-03T15:00:00"
        and: "withing hours"
            // should narrow all to 15:00:00
            "2020-01-03T15:00:00" | Duration.of(1, ChronoUnit.HOURS) | "2020-01-03T15:00:00"
            "2020-01-03T15:00:01" | Duration.of(1, ChronoUnit.HOURS) | "2020-01-03T15:00:00"
            "2020-01-03T15:30:00" | Duration.of(1, ChronoUnit.HOURS) | "2020-01-03T15:00:00"
            "2020-01-03T15:59:59" | Duration.of(1, ChronoUnit.HOURS) | "2020-01-03T15:00:00"
            // and then step to 16:00
            "2020-01-03T16:00:00" | Duration.of(1, ChronoUnit.HOURS) | "2020-01-03T16:00:00"
            // two hours cycle narrow
            "2020-01-03T15:30:30" | Duration.of(2, ChronoUnit.HOURS) | "2020-01-03T14:00:00"
            // also two hours narrow but 2-h cycle starts from 16'th hour so we adapt to elapsed time frame
            "2020-01-03T16:30:30" | Duration.of(2, ChronoUnit.HOURS) | "2020-01-03T16:00:00"
        and: "within days"
            // should narrow all to 2020-01-03T00:00:00
            "2020-01-03T00:00:00" | Duration.of(1, ChronoUnit.DAYS) | "2020-01-03T00:00:00"
            "2020-01-03T00:00:01" | Duration.of(1, ChronoUnit.DAYS) | "2020-01-03T00:00:00"
            "2020-01-03T15:30:30" | Duration.of(1, ChronoUnit.DAYS) | "2020-01-03T00:00:00"
            "2020-01-03T23:59:59" | Duration.of(1, ChronoUnit.DAYS) | "2020-01-03T00:00:00"
            // and then step to 2020-01-04T00:00:00
            "2020-01-04T00:00:00" | Duration.of(1, ChronoUnit.DAYS) | "2020-01-04T00:00:00"
            // two days cycle narrow
            "2020-01-03T15:30:30" | Duration.of(2, ChronoUnit.DAYS) | "2020-01-03T00:00:00"
            "2020-01-02T23:59:59" | Duration.of(2, ChronoUnit.DAYS) | "2020-01-01T00:00:00"
    }

    def "should correctly append to already existing log file"()
    {
        given: "create queue with one minute file cycle"
            ZonedDateTime time = ZonedDateTime.of(LocalDateTime.parse("2020-01-03T00:00:00"), ZoneId.of("UTC"))
            LogFileConfiguration logFileConfiguration = LogFileConfiguration
                    .builder()
                    .path(TestQueueUtil.PATH)
                    .fileCycleDuration(Duration.of(1, ChronoUnit.MINUTES))
                    .fileCycleClock(Clock.fixed(time.toInstant(), ZoneId.of("UTC")))
                    .disableCompression(true)
                    .build()
            MPMCQueueStats<Integer> queue = TestQueueUtil.createQueue(logFileConfiguration)
        when: "we put 2 elements to queue"
            queue.add(5)
            queue.add(5)
            queue.close()
        then: "there are two elements in file"
            Path logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
            Files.readAllBytes(logFile).length == DefaultFileAccess.PROBE_AND_TIMESTAMP_BYTES_SUM * 2
        when: "we create new queue with timestamp that matches to previous 00:00:00 one-minute window"
            time = ZonedDateTime.of(LocalDateTime.parse("2020-01-03T00:00:30"), ZoneId.of("UTC"))
            logFileConfiguration = LogFileConfiguration
                    .builder()
                    .path(TestQueueUtil.PATH)
                    .fileCycleDuration(Duration.of(1, ChronoUnit.MINUTES))
                    .fileCycleClock(Clock.fixed(time.toInstant(), ZoneId.of("UTC")))
                    .build()
            queue = TestQueueUtil.createQueue(logFileConfiguration)
        and: "we put 3rd element"
            queue.add(5)
            queue.close()
            logFile = TestQueueUtil.findExactlyOneOrThrow(TestQueueUtil.PATH)
        then: "there are 3 elements in file"
            Files.readAllBytes(logFile).length == DefaultFileAccess.PROBE_AND_TIMESTAMP_BYTES_SUM * 3
    }

    def "should correctly handle situation when recycle happens between writes to queue"()
    {
        given: "create queue with one minute file cycle"
            ZonedDateTime time = ZonedDateTime.of(LocalDateTime.parse("2020-01-03T00:00:00"), ZoneId.of("UTC"))
            LogFileConfiguration logFileConfiguration = LogFileConfiguration
                    .builder()
                    .path(TestQueueUtil.PATH)
                    .fileCycleDuration(Duration.of(1, ChronoUnit.MINUTES))
                    .fileCycleClock(Clock.fixed(time.toInstant(), ZoneId.of("UTC")))
                    .disableCompression(true)
                    .build()
            MPMCQueueStats<Integer> queue = TestQueueUtil.createQueue(logFileConfiguration)
        when: "we put 2 elements to queue"
            queue.add(5)
            queue.add(5)
        and: "we set time one cycle ahead"
            time = ZonedDateTime.of(LocalDateTime.parse("2020-01-03T00:01:00"), ZoneId.of("UTC"))
            queue.setFileCycleClock(Clock.fixed(time.toInstant(), ZoneId.of("UTC")))
        and: "we put one more element"
            queue.add(5)
            queue.close()
        then: "it should create second file representing next one-minute cycle"
            List<Path> logFiles = TestQueueUtil.findMany(TestQueueUtil.PATH).sort { it.toString() }
            logFiles.size() == 2
        and: "files have correct names"
            logFiles.get(0).toString() == PathDateFormatter.appendDate(TestQueueUtil.PATH, ZonedDateTime.of(LocalDateTime.parse("2020-01-03T00:00:00"), ZoneId.of("UTC"))).toString()
            logFiles.get(1).toString() == PathDateFormatter.appendDate(TestQueueUtil.PATH, ZonedDateTime.of(LocalDateTime.parse("2020-01-03T00:01:00"), ZoneId.of("UTC"))).toString()
        and: "there are two elements in first file"
            Path firstCycleLogFile = logFiles.get(0)
            Files.readAllBytes(firstCycleLogFile).length == DefaultFileAccess.PROBE_AND_TIMESTAMP_BYTES_SUM * 2
        and: "one element in next file cycle"
            Path secondCycleLogFile = logFiles.get(1)
            Files.readAllBytes(secondCycleLogFile).length == DefaultFileAccess.PROBE_AND_TIMESTAMP_BYTES_SUM
    }

}