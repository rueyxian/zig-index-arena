const std = @import("std");
const builtin = @import("builtin");
const debug = std.debug;
const math = std.math;
const meta = std.meta;
const mem = std.mem;
const print = std.debug.print;
const Allocator = std.mem.Allocator;

// pub const Processor = union(enum) {
//     scalar: u8,
//     vector: u8,
//     fn lane_count(processor: Processor) u8 {
//         return switch (processor) {
//             inline else => |n| n,
//         };
//     }
// };

pub fn IndexArena(comptime T: type) type {
    return struct {
        allocator: Allocator,
        unmanaged: Unmanaged,

        const Self = @This();
        pub const Unmanaged = IndexArenaUnmanaged(T);
        pub const Key = Unmanaged.Key;
        pub const Value = Unmanaged.Value;
        pub const Size = Unmanaged.Size;

        pub const Entry = Unmanaged.Entry;
        pub const Iterator = Unmanaged.Iterator;
        pub const KeyIterator = Unmanaged.KeyIterator;
        pub const ValueIterator = Unmanaged.ValueIterator;

        pub fn init(allocator: Allocator) Self {
            return Self{
                .allocator = allocator,
                .unmanaged = Unmanaged{},
            };
        }

        pub fn deinit(self: *Self) void {
            self.unmanaged.deinit(self.allocator);
        }

        pub fn iterator(self: *Self) Iterator {
            return self.unmanaged.iterator();
        }

        pub fn key_iterator(self: *Self) KeyIterator {
            return self.unmanaged.key_iterator();
        }

        pub fn value_iterator(self: *Self) ValueIterator {
            return self.unmanaged.value_iterator();
        }

        pub fn get(self: *const Self, key: Key) Value {
            return self.unmanaged.get(key);
        }

        pub fn get_ptr(self: *const Self, key: Key) *Value {
            return self.unmanaged.get_ptr(key);
        }

        pub fn insert(self: *Self, value: Value) Allocator.Error!Key {
            return self.unmanaged.insert(self.allocator, value);
        }

        pub fn insert_assume_capacity(self: *Self, value: Value) Key {
            return self.unmanaged.insert_assume_capacity(value);
        }

        pub fn remove(self: *Self, key: Key) void {
            self.unmanaged.remove(key);
        }

        pub fn fetch_remove(self: *Self, key: Key) Value {
            return self.unmanaged.fetch_remove(key);
        }

        fn _debug_print_tags(self: *Self) void {
            self.unmanaged._debug_print_tags();
        }
    };
}

// TODO handle zero-sized T

pub fn IndexArenaUnmanaged(
    comptime T: type,
    // comptime processor: Processor,
) type {

    // debug.assert();
    // comptime switch (processor) {
    //     inline else => |n| {
    //         debug.assert(@popCount(n) == 1 and n >= 8);
    //     },
    // };

    return struct {
        memory: ?[*]u8 = null,

        const Self = @This();

        pub const Key = struct {
            index: Size,
        };
        pub const Value = T;
        pub const Size = u32;

        // const LANE_COUNT = switch (processor) {
        //     .scalar => |n| n,
        //     .vector => |n| n,
        // };

        const MIN_CAPCITY: Size = @max(8, @sizeOf(Group(usize)));
        const MAX_SIZE: Size = math.maxInt(Size) - 1;
        const NO_NEXT_FREE = math.maxInt(Size);

        const ALIGNMENT = blk: {
            const header_align = @alignOf(Header);
            const size_align = @alignOf(Header);
            const value_align = if (@sizeOf(Value) == 0) 1 else @alignOf(Value);
            break :blk @max(header_align, size_align, value_align); // TODO including tag group's alignment
        };

        const SIZE_OF_SLOT = @max(@sizeOf(Size), @sizeOf(Value));
        const ALIGN_OF_SLOT = @max(@alignOf(Size), @alignOf(Value));

        const Header = struct {
            next_free: Size = NO_NEXT_FREE,
            size: Size = 0,
            slots: [*]Slot,
            capacity: Size,
        };

        const Tag = u1;
        const TAG_FREE = 0;
        const TAG_USED = 1;

        pub const Entry = struct {
            key: Key,
            value_ptr: *Value,
        };

        pub const Iterator = struct {
            arena: ?*const Self,
            group_count: Size = 0,
            mask: Group(usize) = 0,

            pub fn next(it: *Iterator) ?Entry {
                const arena = it.arena orelse return null;
                const GROUP_COUNT_MAX = @divFloor(arena.capacity() - 1, @bitSizeOf(usize)) + 1;
                while (it.mask == 0) : (it.group_count += 1) {
                    if (it.group_count >= GROUP_COUNT_MAX) {
                        it.arena = null;
                        return null;
                    }
                    it.mask = (arena.tag_groups(usize) + it.group_count)[0];
                }
                defer it.mask &= it.mask - 1;
                const index: Size = ((it.group_count - 1) * @sizeOf(Group(usize))) + @ctz(it.mask);
                debug.assert(arena.get_tag(index) == TAG_USED);
                const key = Key{ .index = index };
                const value_ptr: *Value = @ptrCast(@alignCast(arena.slots() + index));
                return Entry{
                    .key = key,
                    .value_ptr = value_ptr,
                };
            }
        };

        pub const KeyIterator = struct {
            arena: ?*const Self,
            base_idx: Size = 0,
            mask: Group(usize) = 0,

            pub fn next(it: *KeyIterator) ?Key {
                const arena = it.arena orelse return null;
                const GROUP_COUNT_MAX = @divFloor(arena.capacity() - 1, @bitSizeOf(usize)) + 1;
                while (it.mask == 0) : (it.group_count += 1) {
                    if (it.group_count >= GROUP_COUNT_MAX) {
                        it.arena = null;
                        return null;
                    }
                    it.mask = (arena.tag_groups(usize) + it.group_count)[0];
                }
                defer it.mask &= it.mask - 1;
                const index: Size = ((it.group_count - 1) * @sizeOf(Group(usize))) + @ctz(it.mask);
                debug.assert(arena.get_tag(index) == TAG_USED);
                return Key{ .index = index };
            }
        };

        pub const ValueIterator = struct {
            arena: ?*const Self,
            base_idx: Size = 0,
            mask: Group(usize) = 0,

            pub fn next(it: *ValueIterator) ?*Value {
                const arena = it.arena orelse return null;
                const GROUP_COUNT_MAX = @divFloor(arena.capacity() - 1, @bitSizeOf(usize)) + 1;
                while (it.mask == 0) : (it.group_count += 1) {
                    if (it.group_count >= GROUP_COUNT_MAX) {
                        it.arena = null;
                        return null;
                    }
                    it.mask = (arena.tag_groups(usize) + it.group_count)[0];
                }
                defer it.mask &= it.mask - 1;
                const index: Size = ((it.group_count - 1) * @sizeOf(Group(usize))) + @ctz(it.mask);
                debug.assert(arena.get_tag(index) == TAG_USED);
                return @ptrCast(@alignCast(arena.slots() + index));
            }
        };

        fn Group(comptime Int: type) type {
            const info = @typeInfo(Int);
            debug.assert(info == .Int);
            debug.assert(info.Int.signedness == .unsigned);
            debug.assert(@popCount(info.Int.bits) == 1);
            return Int;
        }

        const Slot align(@max(@alignOf(Size), @alignOf(Value))) = [@max(@sizeOf(Size), @sizeOf(Value))]u8;
        comptime {
            debug.assert(@alignOf(Header) != 0);
            debug.assert(@alignOf(Tag) != 0);
            debug.assert(@alignOf(Slot) != 0);
        }

        fn header(self: *const Self) *Header {
            return @ptrCast(@as([*]Header, @ptrCast(@alignCast(self.memory.?))) - 1);
        }

        // fn tags(self: *const Self) [*]Tag {
        //     return @ptrCast(@alignCast(self.memory.?));
        // }

        fn tag_groups(self: *const Self, comptime Int: type) [*]Group(Int) {
            return @ptrCast(@alignCast(self.memory.?));
        }

        fn slots(self: *const Self) [*]Slot {
            return self.header().slots;
        }

        fn size(self: *const Self) Size {
            if (self.memory == null) return 0;
            return self.header().size;
        }

        fn capacity(self: *const Self) Size {
            if (self.memory == null) return 0;
            return self.header().capacity;
        }

        fn allocate(self: *Self, allocator: Allocator, new_capacity: Size) Allocator.Error!void {
            debug.assert(new_capacity >= MIN_CAPCITY);

            const tags_size = (@divFloor((@bitSizeOf(Tag) * new_capacity) - 1, @bitSizeOf(Group(usize))) + 1) * @sizeOf(Group(usize));
            const tags_lo: usize = mem.alignForward(usize, @sizeOf(Header), @alignOf(Group(usize)));
            const tags_hi: usize = tags_lo + tags_size;

            const slots_size = @sizeOf(Slot) * new_capacity;
            const slots_lo: usize = mem.alignForward(usize, tags_hi, ALIGN_OF_SLOT);
            const slots_hi: usize = slots_lo + slots_size;

            const total_size: usize = mem.alignForward(usize, slots_hi, ALIGNMENT);

            const memory = try allocator.alignedAlloc(u8, ALIGNMENT, total_size);
            const addr = @intFromPtr(memory.ptr);

            const hdr: *Header = @ptrCast(@as([*]Header, @ptrCast(@alignCast(memory))));
            hdr.* = Header{
                .slots = @ptrFromInt(addr + slots_lo),
                .capacity = new_capacity,
            };
            self.memory = @ptrFromInt(addr + @sizeOf(Header));
        }

        fn deallocate(self: *Self, allocator: Allocator) void {
            if (self.memory == null) return;
            const cap = self.capacity();

            const tags_size = (@divFloor((@bitSizeOf(Tag) * cap) - 1, @bitSizeOf(Group(usize))) + 1) * @sizeOf(Group(usize));
            const tags_lo: usize = mem.alignForward(usize, @sizeOf(Header), @alignOf(Group(usize)));
            const tags_hi: usize = tags_lo + tags_size;

            const slots_size = @sizeOf(Slot) * cap;
            const slots_lo: usize = mem.alignForward(usize, tags_hi, ALIGN_OF_SLOT);
            const slots_hi: usize = slots_lo + slots_size;

            const total_size: usize = mem.alignForward(usize, slots_hi, ALIGNMENT);

            const addr: usize = @intFromPtr(self.memory.?) - @sizeOf(Header);
            const memory = @as([*]align(ALIGNMENT) u8, @ptrFromInt(addr))[0..total_size];

            allocator.free(memory);
        }

        // fn init_tags(self: *Self) void {
        //     debug.assert(self.capacity() >= MIN_CAPCITY);
        //     const group_count = @divFloor(self.capacity() - 1, @bitSizeOf(Group(usize))) + 1;
        //     const slice = self.tag_groups(usize)[0..group_count];
        //     @memset(slice, TAG_FREE);
        // }

        fn create_key(self: *Self) Key {
            debug.assert(self.size() < self.capacity());
            debug.assert(self.size() < MAX_SIZE);
            const index = blk: {
                const next_free = self.header().next_free;
                if (next_free == NO_NEXT_FREE) {
                    break :blk self.size();
                }
                defer self.header().next_free = @bitCast((self.slots() + next_free)[0]);
                break :blk next_free;
            };
            self.set_tag_used(index);
            self.header().size += 1;
            return Key{ .index = index };
        }

        fn destroy_key(self: *Self, key: Key) void {
            debug.assert(self.size() != 0);
            const index = key.index;
            const slot: *Slot = @ptrCast(self.slots() + index);
            slot.* = @bitCast(self.header().next_free);
            self.header().next_free = index;
            self.set_tag_free(index);
            self.header().size -= 1;
        }

        fn get_tag(self: Self, index: Size) Tag {
            const base_idx = @divFloor(index, @bitSizeOf(u8));
            const sub_idx = index % @bitSizeOf(u8);
            const mask = (@as(u8, 1) << @as(math.Log2Int(u8), @intCast(sub_idx)));
            const group = (self.tag_groups(u8) + base_idx)[0];
            return @intFromBool(group & mask != 0);
        }

        fn set_tag_used(self: *Self, index: Size) void {
            debug.assert(self.get_tag(index) == TAG_FREE);
            const base_idx = @divFloor(index, @bitSizeOf(u8));
            const sub_idx = index % @bitSizeOf(u8);
            const mask = @as(u8, 1) << @as(math.Log2Int(u8), @intCast(sub_idx));
            const group = &(self.tag_groups(u8) + base_idx)[0];
            group.* |= mask;
        }

        fn set_tag_free(self: *Self, index: Size) void {
            debug.assert(self.get_tag(index) == TAG_USED);
            const base_idx = @divFloor(index, @bitSizeOf(u8));
            const sub_idx = index % @bitSizeOf(u8);
            const mask = ~(@as(u8, 1) << @as(math.Log2Int(u8), @intCast(sub_idx)));
            const group = &(self.tag_groups(u8) + base_idx)[0];
            group.* &= mask;
        }

        fn better_capacity(new_capacity: Size) Size {
            return @max(MIN_CAPCITY, ceil_power_of_two(Size, new_capacity));
        }

        fn grow_precise(self: *Self, allocator: Allocator, new_capacity: Size) !void {
            @setCold(true);
            debug.assert(new_capacity > self.capacity());
            debug.assert(new_capacity >= MIN_CAPCITY);

            var new_arena = Self{};
            defer new_arena.deinit(allocator);
            try new_arena.allocate(allocator, new_capacity);

            if (self.capacity() != 0) {
                new_arena.header().size = self.size();
                {
                    const len: usize = @divFloor((new_capacity - self.capacity()) - 1, @bitSizeOf(Group(usize))) + 1;
                    const dst_tag_groups = new_arena.tag_groups(usize)[0..len];
                    const src_tag_groups = self.tag_groups(usize)[0..len];
                    @memcpy(dst_tag_groups, src_tag_groups);
                }
                {
                    const len = self.capacity();
                    const dst_slots = new_arena.slots()[0..len];
                    const src_slots = self.slots()[0..len];
                    @memcpy(dst_slots, src_slots);
                }
            }
            {
                const lo = @intFromBool(self.capacity() != 0) * (@divFloor(self.capacity() -| 1, @bitSizeOf(Group(usize))) + 1);
                const hi = @divFloor(new_capacity - 1, @bitSizeOf(Group(usize))) + 1;
                const dst_tag_groups = new_arena.tag_groups(usize)[lo..hi];
                @memset(dst_tag_groups, TAG_FREE);
            }
            mem.swap(Self, self, &new_arena);
        }

        fn grow_if_needed(self: *Self, allocator: Allocator, required_additional: Size) !void {
            const new_capacity = better_capacity(self.size() + required_additional);
            if (new_capacity > self.capacity()) {
                try self.grow_precise(allocator, new_capacity);
            }
        }

        pub fn deinit(self: *Self, allocator: Allocator) void {
            self.deallocate(allocator);
            self.* = undefined;
        }

        pub fn iterator(self: *const Self) Iterator {
            if (self.memory == null) {
                return Iterator{ .arena = null };
            }
            return Iterator{ .arena = self };
        }

        pub fn key_iterator(self: *const Self) KeyIterator {
            if (self.memory == null) {
                return KeyIterator{ .arena = null };
            }
            return KeyIterator{ .arena = self };
        }

        pub fn value_iterator(self: *const Self) ValueIterator {
            if (self.memory == null) {
                return ValueIterator{ .arena = null };
            }
            return ValueIterator{ .arena = self };
        }

        pub fn get(self: *const Self, key: Key) Value {
            const index = key.index;
            debug.assert(self.get_tag(index) == 1);
            const slot = (self.slots() + index)[0];
            return @bitCast(slot);
        }

        pub fn get_ptr(self: *Self, key: Key) *Value {
            const index = key.index;
            debug.assert(self.get_tag(index) == 1);
            const slot_ptr = self.slots() + index;
            return @ptrCast(@alignCast(slot_ptr));
        }

        pub fn insert(self: *Self, allocator: Allocator, value: Value) Allocator.Error!Key {
            try self.grow_if_needed(allocator, 1);
            return self.insert_assume_capacity(value);
        }

        pub fn insert_assume_capacity(self: *Self, value: Value) Key {
            const key = self.create_key();
            const slot = self.slots() + key.index;
            slot[0] = @bitCast(value);
            return key;
        }

        pub fn remove(self: *Self, key: Key) void {
            self.destroy_key(key);
        }

        pub fn fetch_remove(self: *Self, key: Key) Value {
            defer self.destroy_key(key);
            return @bitCast((self.slots() + key.index)[0]);
        }

        fn _debug_print_tags(self: *Self) void {
            var base_idx: usize = 0;
            const COUNT: usize = @intFromBool(self.capacity() != 0) * (@divFloor(self.capacity() -| 1, @bitSizeOf(usize)) + 1);

            if (COUNT == 0) {
                print("groups: null\n\n", .{});
                return;
            }

            while (base_idx < COUNT) : (base_idx += 1) {
                if (base_idx == 0) {
                    print("groups: ", .{});
                } else {
                    print("        ", .{});
                }
                var group: Group(usize) = (self.tag_groups(usize) + base_idx)[0];
                group = @byteSwap(group);
                for (0..@bitSizeOf(Group(usize))) |i| {
                    const mask: Group(usize) = @as(Group(usize), 1) << @as(math.Log2Int(Group(usize)), @intCast(i));
                    if (group & mask == 0) {
                        print("0", .{});
                    } else {
                        print("1", .{});
                    }
                    if ((i + 1) % 8 == 0) {
                        print(" ", .{});
                    }
                }
                print("\n", .{});
            }

            print("\n", .{});
        }
    };
}

fn ceil_power_of_two(comptime T: type, value: T) T {
    const LOOKUP = power_of_two_table(T);
    const log2 = math.log2_int_ceil(T, value);
    return LOOKUP[log2];
}

fn power_of_two_table(comptime T: type) PowerOfTwoTable(T) {
    comptime var arr: PowerOfTwoTable(T) = undefined;
    inline for (0..arr.len) |i| {
        arr[i] = @as(T, 1) << @as(math.Log2Int(T), @intCast(i));
    }
    return arr;
}

fn PowerOfTwoTable(comptime T: type) type {
    const info = @typeInfo(T);
    debug.assert(info == .Int);
    debug.assert(info.Int.signedness == .unsigned);
    const bits = info.Int.bits;
    return [bits]T;
}

const testing = std.testing;

test "basic" {
    //

    const allocator: Allocator = testing.allocator;

    var arena = IndexArena(u32).init(allocator);
    defer arena.deinit();
}

test "iterator" {
    //

    const allocator: Allocator = testing.allocator;

    var arena = IndexArena(u32).init(allocator);
    defer arena.deinit();

    const k0 = try arena.insert(0);
    const k1 = try arena.insert(1);
    const k2 = try arena.insert(2);
    const k3 = try arena.insert(3);
    const k4 = try arena.insert(4);
    const k5 = try arena.insert(5);
    const k6 = try arena.insert(6);
    const k7 = try arena.insert(7);
    const k8 = try arena.insert(8);
    const k9 = try arena.insert(9);
    const k10 = try arena.insert(10);
    _ = k0;
    _ = k2;
    _ = k3;
    _ = k5;
    _ = k6;
    _ = k7;
    _ = k10;

    arena.remove(k1);
    arena.remove(k4);
    arena.remove(k8);
    arena.remove(k9);

    {
        var it = arena.iterator();
        while (it.next()) |e| {
            print("{} | {}\n", .{ e.key, e.value_ptr.* });
        }

        // print("{any} \n", .{it.next()});
        // print("{any} \n", .{it.next()});
        // print("{any} \n", .{it.next()});
        // print("{any} \n", .{it.next()});
        // print("{any} \n", .{it.next()});
        // print("{any} \n", .{it.next()});
        // print("{any} \n", .{it.next()});
        // print("{any} \n", .{it.next()});
    }

    arena._debug_print_tags();
}

test "draft" {
    if (true) return error.SkipZigTest;

    print("\n", .{});

    const allocator: Allocator = testing.allocator;
    // const allocator: Allocator = std.heap.page_allocator;

    // const proc = Processor{ .vector = 16 };

    const Arena = IndexArenaUnmanaged(u32);

    var arena = Arena{};
    defer arena.deinit(allocator);

    // try arena.allocate(allocator, 16);
    // arena.init_tags();

    {
        // print("res: {any}\n", .{div_ceil(28, 4)});
        // print("res: {any}\n", .{div_ceil(29, 4)});
        // print("res: {any}\n", .{div_ceil(30, 4)});
        // print("res: {any}\n", .{div_ceil(31, 4)});
        // print("res: {any}\n", .{div_ceil(32, 4)});
        // print("res: {any}\n", .{div_ceil(33, 4)});
    }

    arena._debug_print_tags();

    // _ = arena.insert_assume_capacity(3);
    // _ = arena.insert_assume_capacity(3);
    // _ = arena.insert_assume_capacity(3);
    // _ = arena.insert_assume_capacity(3);
    // _ = arena.insert_assume_capacity(3);
    // _ = arena.insert_assume_capacity(3);
    // _ = arena.insert_assume_capacity(3);
    // _ = arena.insert_assume_capacity(3);
    // _ = arena.insert_assume_capacity(3);
    // _ = arena.insert_assume_capacity(3);
    // _ = arena.insert_assume_capacity(3);
    // _ = arena.insert_assume_capacity(3);

    {
        // try arena.grow_precise(allocator, 68);
        // try arena.grow_if_needed(allocator, 1);

        // const k1 = arena.insert_assume_capacity(3);
        // _ = k1;
        // // _ = k1;
        // const k2 = arena.insert_assume_capacity(6);
        // _ = k2; // _ = k2;
        // const k3 = arena.insert_assume_capacity(9);
        // _ = k3;
        // // _ = k3;
        // const k4 = arena.insert_assume_capacity(12);
        // _ = k4;
        // const k5 = arena.insert_assume_capacity(15);
        // _ = k5;
        // const k6 = arena.insert_assume_capacity(18);
        // _ = k6;
        // const k7 = arena.insert_assume_capacity(21);
        // _ = k7;
        // const k8 = arena.insert_assume_capacity(24);
        // const k9 = arena.insert_assume_capacity(27);
        // _ = k9;
        // const k10 = arena.insert_assume_capacity(30);
        // _ = k10;
        // const k11 = arena.insert_assume_capacity(33);
        // _ = k11;
        // _ = k8;

        // print("remove: {}\n", .{arena.fetch_remove(k3)});
        // print("remove: {}\n", .{arena.fetch_remove(k6)});

        // const k9 = arena.insert_assume_capacity(27);
        // const slots = arena.slots()[0..arena.capacity()];
        // _ = slots;
        // const slice = m[0..]

        // arena.get_tag_group(, )
        // {
        //     const group = Arena.tag_group(proc, arena.memory.?);
        //     print("tag group: {any}\n", .{group});
        // }

        // try arena.grow(allocator, 32);

        // arena._debug_print_tags();

        for (0..140) |i| {
            const n: u32 = @intCast(i);
            _ = try arena.insert(allocator, n);
            arena._debug_print_tags();
            // _ = arena.insert_assume_capacity(n);
        }

        // try arena.grow_if_needed(allocator, 1);
        // print("size: {}\n", .{arena.size()});
        // print("capacity: {}\n", .{arena.capacity()});

        {
            // const group = arena.tag_groups(usize);
            // print("group: {b:0>64}\n", .{group[0]});
        }
    }
}
