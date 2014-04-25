class cassandra {

  $keyid = '4BD736A82B5C1B00'
  $shortkey = '2048R/2B5C1B00'

  file { '/etc/apt/sources.list.d/cassandra.list':
    content => 'deb http://www.apache.org/dist/cassandra/debian 20x main'
  }

  exec { 'cassandra-recv-keys':
    command => "gpg --keyserver pgp.mit.edu --recv-keys ${keyid} && gpg --export --armor ${keyid} | apt-key add - && apt-get update",
    user => 'root',
    group => 'root',
    path => "/bin:/usr/bin:/sbin:/usr/sbin",
    unless =>"apt-key list | grep ${shortkey}",
    require => File['/etc/apt/sources.list.d/cassandra.list']
  }

  $cassandra_heap_size = hiera('cassandra-heap-size', '8G')
  $cassandra_heap_new = hiera('cassandra-heap-new', '800m')

  package { 'cassandra':
    ensure => present,
    require => Exec['cassandra-recv-keys']
  }

  file { '/etc/cassandra/cassandra.yaml':
    content => template('cassandra/cassandra.yaml.erb'),
    require => Package['cassandra'],
    notify => Service['cassandra']
  }

  file { '/etc/default/cassandra':
    content => template('cassandra/cassandra.env.erb'),
    require => Package['cassandra'],
    notify => Service['cassandra']
  }

  service {'cassandra':
    ensure => running
  }
}
