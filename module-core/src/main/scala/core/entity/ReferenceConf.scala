package core.entity

case class ReferenceConf(dbUrl: String,
                         dbDriverClassName: String,
                         dbUsername: String,
                         dbPassword: String,
                         dsMaxTotal: Int,
                         dsMaxIdle: Int,
                         dsMinIdle: Int,
                         dsMaxWaitMillis: Long,
                         tpsMetric: String)
