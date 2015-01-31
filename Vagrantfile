# vi: set ft=ruby :

Vagrant.configure(2) do |config|
    config.vm.box = "ubuntu/trusty64"

    config.vm.define "test" do |test|
        test.vm.hostname = "test.mydiff.dev"
        test.vm.network "private_network", ip: "192.168.77.7"

        test.vm.provision :shell, path: "vagrant/root.sh"
    end
end
