defmodule ClientDynamoTest do
    use ExUnit.Case
    doctest Dynamo

    import Emulation, only: [spawn: 2, send: 2]

    import Kernel,
      except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

    test "Client is Able to Read and Write at Dynamo" do
        Emulation.init()
        Emulation.append_fuzzers([Fuzzers.delay(2)])
        view = %{a: true,b: true, c: true,d: true,e: true,f: true, g: true, h: true,i: true, j: true}
        key_value_map = %{}
        r = 3
        w = 1
        n = 5
        gossip_timeout = 2000
        node_list = [:a, :b, :c, :d, :e, :f, :g, :h, :i, :j]
        key_range = %{a: [{0,99} , {200,299}, {500, 599}, {800,899},{900,999}],b: [{0,99},{100,199},{200,299}, {600, 699}, {700, 799}],c: [{0,99},{100,199},{200,299}, {500, 599}, {800,899}],d: [{200,299},{300,399},{100,199}, {500, 599}, {800,899}],e: [{200,299},{300,399},{400,499}, {500, 599}, {900,999}],f: [{300,399},{400,499},{500,599}, {700, 799}, {800,899}],g: [{400,499},{500,599},{600,699}, {700, 799}, {800,899}],h: [{500,599},{600,699},{700,799}, {800, 899}, {900,999}],i: [{200, 299}, {600,699},{700,799},{800,899}, {900, 999}],j: [{300, 399}, {400, 499}, {700,799},{800,899},{900,999}]}
        base_config = Dynamo.new_configuration(view,key_value_map, r,w,n, node_list,key_range_data, gossip_timeout)
        spawn(:a, fn -> Dynamo.startDynamo(base_config) end)
        spawn(:b, fn -> Dynamo.startDynamo(base_config) end)
        spawn(:c, fn -> Dynamo.startDynamo(base_config) end)
        spawn(:d, fn -> Dynamo.startDynamo(base_config) end)
        spawn(:e, fn -> Dynamo.startDynamo(base_config) end)
        spawn(:f, fn -> Dynamo.startDynamo(base_config) end)
        spawn(:g, fn -> Dynamo.startDynamo(base_config) end)
        spawn(:h, fn -> Dynamo.startDynamo(base_config) end)
        spawn(:i, fn -> Dynamo.startDynamo(base_config) end)
        spawn(:j, fn -> Dynamo.startDynamo(base_config) end)
        receive do
        after
            1_000 -> :ok
        end

        client = spawn(:client, fn ->
            client = Dynamo.Client.new_client(:a)
            t1 = Emulation.now()
            {:ok, client} = Dynamo.Client.put(client, 10, 1, 1)
            t2 = Emulation.now()
            t = t2 - t1
            t = Emulation.emu_to_millis(t)
            {:ok, client} = Dynamo.Client.put(client, 10, 1, 2)
            client = Dynamo.Client.update_node(client,:c)
            {{value,staleness}, client} = Dynamo.Client.get(client, 10)
            assert value == 1
          end)
        handle = Process.monitor(client)
        receive do
            {:DOWN, ^handle, _, _, _} -> true
        after
            60_000 -> assert false
        end
    after
        Emulation.terminate()
    end


    test "Staleness When R+W = N" do
        Emulation.init()
        Emulation.append_fuzzers([Fuzzers.delay(2)])
        view = %{a: true,b: true, c: true,d: true,e: true,f: true, g: true, h: true,i: true, j: true}
        key_value_map = %{}
        r = 3
        w = 2
        n = 5
        gossip_timeout = 2000
        node_list = [:a, :b, :c, :d, :e, :f, :g, :h, :i, :j]
        key_range = %{a: [{0,99} , {200,299}, {500, 599}, {800,899},{900,999}],b: [{0,99},{100,199},{200,299}, {600, 699}, {700, 799}],c: [{0,99},{100,199},{200,299}, {500, 599}, {800,899}],d: [{200,299},{300,399},{100,199}, {500, 599}, {800,899}],e: [{200,299},{300,399},{400,499}, {500, 599}, {900,999}],f: [{300,399},{400,499},{500,599}, {700, 799}, {800,899}],g: [{400,499},{500,599},{600,699}, {700, 799}, {800,899}],h: [{500,599},{600,699},{700,799}, {800, 899}, {900,999}],i: [{200, 299}, {600,699},{700,799},{800,899}, {900, 999}],j: [{300, 399}, {400, 499}, {700,799},{800,899},{900,999}]}
        base_config = Dynamo.new_configuration(view,key_value_map, r,w,n, node_list,key_range_data, gossip_timeout)
        spawn(:a, fn -> Dynamo.startDynamo(base_config) end)
        spawn(:b, fn -> Dynamo.startDynamo(base_config) end)
        spawn(:c, fn -> Dynamo.startDynamo(base_config) end)
        spawn(:d, fn -> Dynamo.startDynamo(base_config) end)
        spawn(:e, fn -> Dynamo.startDynamo(base_config) end)
        spawn(:f, fn -> Dynamo.startDynamo(base_config) end)
        spawn(:g, fn -> Dynamo.startDynamo(base_config) end)
        spawn(:h, fn -> Dynamo.startDynamo(base_config) end)
        spawn(:i, fn -> Dynamo.startDynamo(base_config) end)
        spawn(:j, fn -> Dynamo.startDynamo(base_config) end)
        receive do
        after
            1_000 -> :ok
        end

        client = spawn(:client, fn ->
            staleCount = 0
            totalCount = 0
            client = Dynamo.Client.new_client(:a)
            {:ok, client} = Dynamo.Client.put(client, 1, 1, 1)
            client = Dynamo.Client.update_node(client,:f)
            {:ok, client} = Dynamo.Client.put(client, 1, 2, 2)
            client = Dynamo.Client.update_node(client,:c)
            {:ok, client} = Dynamo.Client.put(client, 1, 1, 3)
            client = Dynamo.Client.update_node(client,:d)
            {:ok, client} = Dynamo.Client.put(client, 1, 2, 4)
            client = Dynamo.Client.update_node(client,:b)
            {{value,ret_context}, client} = Dynamo.Client.get(client, 1)

            if ret_context < 4 do
                staleCount = staleCount + 1
            end

            client = Dynamo.Client.update_node(client,:d)
            {:ok, client} = Dynamo.Client.put(client, 1, 1, 5)
            {:ok, client} = Dynamo.Client.put(client, 1, 2, 6)
            client = Dynamo.Client.update_node(client,:f)
            {{value,ret_context}, client} = Dynamo.Client.get(client, 1)
            if ret_context < 6 do
                staleCount = staleCount + 1
            end

        end)
        handle = Process.monitor(client)
        receive do
            {:DOWN, ^handle, _, _, _} -> true
        after
            60_000 -> assert false
        end
    after
        Emulation.terminate()
    end

end
