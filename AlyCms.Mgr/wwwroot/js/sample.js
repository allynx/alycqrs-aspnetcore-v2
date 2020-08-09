; layui.extend({  setter: 'js/config', cpnts: 'lib/extend/cpnts' }).define(["table", 'vue', 'cpnts','setter'],
    function (t) {
        var $ = layui.$,
            cpnts = layui.cpnts,
            table = layui.table,
            setter = layui.setter,
            vm = new layui.vue({
                el: '#editor_windows',
                data: {
                    id: undefined,
                    title: undefined,
                    disable: undefined
                },
                components: {
                    "tl-checkbox": cpnts.checkbox
                }
            });

        function sfour() {
            return (((1 + Math.random()) * 0x10000) | 0).toString(16).substring(1);
        }

        function guid() {
            return (sfour() + sfour() + "-" + sfour() + "-" + sfour() + "-" + sfour() + "-" + sfour() + sfour() + sfour());
        }

        function setVm(m) {
            vm.$data.id = m.id;
            vm.$data.title = m.title;
            vm.$data.disable = m.disable;
        }

        table.render({
            elem: "#aly_app_content_list",
            toolbar: "#table_toolbar",
            height: 'full-123',
            url: setter.request.apiUrl+"/sample/get",
            request: {
                limitName: 'pageSize'
            },
            parseData: function (res) {
                return {
                    "code": 0, //解析接口状态
                    "msg": 'Request is success ...', //解析提示文本
                    "count": res.totalRecords, //解析数据长度
                    "data": res.data, //解析数据列表
                };
            },
            cols: [[{
                type: "checkbox",
                fixed: "left"
            },
            {
                field: "id",
                title: "ID",
                sort: !0
            },
            {
                field: "title",
                title: "标题"
            },
            {
                field: "disable",
                title: "禁用",
                templet: "#btn_tpl",
                minWidth: 80,
                align: "center"
            },
            {
                title: "操作",
                minWidth: 150,
                align: "center",
                fixed: "right",
                toolbar: "#table_tool"
            }]],
            page: true,
            limit: 10,
            limits: [10, 20, 30, 50, 100, 500],
            text: "对不起，加载出现异常！"
        });

        table.on("toolbar(aly_app_content_list)", function (t) {
            if ("batchdel" === t.event) {
                var checkStatus = table.checkStatus('aly_app_content_list'), checkData = checkStatus.data;

                if (checkData.length === 0) {
                    return layer.msg('请选择数据');
                }
                layer.confirm('确定禁用选择的项吗？', function (i, e) {
                    //执行 Ajax 后重载
                    //table.reload('aly_app_content_list');
                    layer.msg('To do batch disable ....');
                });
            }
            if ("add" === t.event) {

                setVm({ id: guid() });

                layer.open({
                    type: 1
                    , title: '添加'
                    , content: $("#editor_windows")
                    , maxmin: true
                    , anim: 4
                    , btn: ['确定', '取消']
                    , yes: function (i, e) {
                        $.ajax({
                            type: "post",
                            url: setter.request.apiUrl +"/sample/add",
                            contentType: "application/json",
                            data: JSON.stringify(vm.$data),
                            success: function (res) {
                                if (res.status) {
                                    table.reload('aly_app_content_list');
                                    layer.close(i);
                                } else {
                                    layer.msg(res.msg);
                                }
                            }, error: function (xhr, ts, et) {
                                layer.alert(xhr.responseText ? xhr.responseText : JSON.stringify(xhr));
                            }
                        });
                    }
                });
            }
        });

        table.on("tool(aly_app_content_list)", function (t) {
            if ("del" === t.event && !t.data.disable) {
                layer.confirm("确定禁用此项吗？", function (i, e) {
                    $.ajax({
                        type: "delete",
                        url: setter.request.apiUrl +"/sample/abolish",
                        contentType: "application/json",
                        data: JSON.stringify({ id: t.data.id }),
                        success: function (res) {
                            if (res.status) {
                                table.reload('aly_app_content_list');
                                layer.close(i);
                            } else {
                                layer.msg(res.msg);
                            }
                        }, error: function (xhr, ts, et) {
                            layer.alert(xhr.responseText ? xhr.responseText : JSON.stringify(xhr));
                        }
                    });
                });
            }
            if ("edit" === t.event) {

                setVm(t.data);

                var edit = layer.open({
                    type: 1,
                    title: "编辑",
                    content: $("#editor_windows"),
                    maxmin: true,
                    anim: 1,
                    btn: ["确定", "取消"],
                    yes: function (i, e) {
                        $.ajax({
                            type: "put",
                            url: setter.request.apiUrl +"/sample/change",
                            contentType: "application/json",
                            data: JSON.stringify(vm.$data),
                            success: function (res) {
                                if (res.status) {
                                    table.reload('aly_app_content_list');
                                    layer.close(i);
                                } else {
                                    layer.msg(res.msg);
                                }
                            }, error: function (xhr, ts, et) {
                                layer.alert(xhr.responseText ? xhr.responseText : JSON.stringify(xhr));
                            }
                        });
                    }
                });
            }
        });

        t("sample", {});
    });