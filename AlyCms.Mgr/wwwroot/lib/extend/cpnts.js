; layui.define(function (exports) {
    exports('cpnts', {
        checkbox: {
            props: {
                value: [Boolean],
                title: [String],
                name: [String],
                skin: [String],
                tip: [Array],
                disable: [Boolean]
            },
            template: `<div class="layui-unselect"
                             v-bind:class="{
                                'layui-checkbox-disbaled':disable,
                                'layui-disabled':disable,
                                'layui-form-switch':skin=='switch',
                                'layui-form-onswitch':value && skin=='switch',
                                'layui-form-checkbox':skin!='switch',
                                'layui-form-checked':value && skin!='switch'
                             }"
                             v-bind:lay-skin="skin=='switch'?'_switch':skin"
                             v-on:click.stop="changeValueHandle">
                            <template v-if="skin=='switch'">
                                <em v-text="tip && tip.length > 1 ? value ? tip[0]:tip[1] :''"></em>
                            </template>
                            <template v-else>
                                <span v-text="title"></span>
                            </template>
                            <i v-bind:class="{'layui-icon':skin!='switch','layui-icon-ok':skin!='switch'}"></i>
                            <input type="hidden" v-bind:name="name" v-model="value" />
                        </div>`,
            methods: {
                changeValueHandle() {
                    if (!this.disable) {
                        this.$emit("input", !this.value);
                    }
                }
            }
        }
    })
});