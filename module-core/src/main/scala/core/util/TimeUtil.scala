package core.util

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset}

object TimeUtil {

    def watchTime[T](block: => T): T = {
        val start = System.currentTimeMillis()
        val res = block
        val end = System.currentTimeMillis()
        val elapsed = (end - start) / 1000
        println(s"[Elapsed time]: $elapsed sec")
        res
    }

    /**
     * @param partition 'yyyyMMdd' formatted String
     */
    def convertPartitionToDateString(partition: String): String = {
        val formatterInput = DateTimeFormatter.ofPattern("yyyyMMdd")
        val formatterOutput = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val parsed = LocalDate.parse(partition, formatterInput)

        parsed.format(formatterOutput)
    }

    /**
     * @param partition 'yyyyMMdd' formatted String
     */
    def convertPartitionToDateSlashString(partition: String): String = {
        val formatterInput = DateTimeFormatter.ofPattern("yyyyMMdd")
        val formatterOutput = DateTimeFormatter.ofPattern("yyyy/MM/dd")
        val parsed = LocalDate.parse(partition, formatterInput)

        parsed.format(formatterOutput)
    }

    /**
     * @param partition 'yyyyMMdd' formatted String
     */
    def convertPartitionToSqlTimestamp(partition: String): java.sql.Timestamp = {
        val formatterInput = DateTimeFormatter.ofPattern("yyyyMMdd")
        val parsed = LocalDate.parse(partition, formatterInput).atStartOfDay()

        java.sql.Timestamp.valueOf(parsed)
    }

    /**
     * @param raw Assume the passed parameter has UTC timezone
     */
    def convertStringToEpochMillis(raw: String): Long = {
        val formatterInput = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val parsed = LocalDateTime.parse(raw.substring(0, 19), formatterInput)

        parsed.atZone(ZoneOffset.UTC).toInstant.toEpochMilli
    }

    def getExpireEpochSeconds(expireDays: Int): Long = {
        val updatedAt = Instant.now().toEpochMilli
        val expireTtl = (updatedAt + (expireDays * 86400 * 1000)) / 1000
        expireTtl
    }
}
