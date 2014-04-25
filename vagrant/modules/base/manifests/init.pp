class base {

  $motd_content = hiera('motd', "no motd set")

  file { '/etc/motd':
    content => "${motd_content}\n"
  }

}
