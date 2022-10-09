package core.model

case class Conf(dbDriverClassName: String,
                dbUrl: String,
                dbUsername: String,
                dbPassword: String,
                dsMaxTotal: Int,
                dsMaxIdle: Int,
                dsMinIdle: Int,
                dsMaxWaitMillis: Long)
