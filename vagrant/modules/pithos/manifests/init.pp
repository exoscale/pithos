class cassandra {

  $keyid = 'F758CE318D77295D'

  exec { 'cassandra-recv-keys':
    command => "gpg --keyserver pgp.mit.edu --recv-keys ${keyid} && gpg --export --armor ${keyid} | apt-key add - && apt-get update",
    user => 'root',
    group => 'root',
    path => "/bin:/usr/bin:/sbin:/usr/sbin",
    unless =>"apt-key list | grep ${keyid}",
  }

  package { 'cassandra':
    ensure => latest,
    require => Exec['cassandra-recv-keys']
  }

}
