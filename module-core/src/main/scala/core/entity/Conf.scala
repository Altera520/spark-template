package core.entity

case class Conf(dbUrl: String,
                dbDriverClassName: String,
                dbUsername: String,
                dbPassword: String,
                dsMaxTotal: Int,
                dsMaxIdle: Int,
                dsMinIdle: Int,
                dsMaxWaitMillis: Long)
