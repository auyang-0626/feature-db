use serde::{ Serialize};


const VALUE_LEN: u64 = 64;


/// 线程安全的位图
#[derive(Debug, Serialize)]
pub struct BitMap {
    size: u64,
    data: Vec<u64>,
}

impl BitMap {
    pub fn new(size: u64) -> BitMap {
        let vec_len = size / VALUE_LEN + 1;
        let data = vec![0; vec_len as usize];
        BitMap {
            size,
            data,
        }
    }
    pub fn set(&mut self, index: u64, value: bool) {
        assert!(index < self.size, "index 必须小于 BitMap size,{},{}", index, self.size);
        let pos = (index / VALUE_LEN) as usize;
        let offset = VALUE_LEN - index % VALUE_LEN - 1;
        let v = self.data[pos];

        if value {
            let mask: u64 = 1 << offset;
            self.data[pos] = v | mask;
        } else {
            let mask = u64::MAX - (1 << offset);
            self.data[pos] = v & mask;
        }
    }

    pub fn get(&self, index: u64) -> bool {
        assert!(index < self.size, "index 必须小于 BitMap size,{},{}", index, self.size);
        let pos = (index / VALUE_LEN) as usize;
        let offset = VALUE_LEN - index % VALUE_LEN -1;
        let v = self.data[pos];

        let mask: u64 = 1 << offset;

        (v & mask) > 0
    }

    pub fn first_false_value(&self) -> Option<u64> {
        for i in 0.. self.size{
            // 很显然，还有更优化的写法，以后再说
            if !self.get(i) {
                return Some(i)
            }
        }
        None
    }
}


#[cfg(test)]
mod tests {
    use crate::tools::bitmap::BitMap;

    #[test]
    pub fn test_new() {
        let mut bit_map = BitMap::new(129);
        println!("{:?}", bit_map);

        let id1 = bit_map.first_false_value().unwrap();
        println!("id:{}",id1);
        bit_map.set(id1, true);


        let id2 = bit_map.first_false_value().unwrap();
        println!("id:{}",id2);
        bit_map.set(id2, true);


        let id3 = bit_map.first_false_value().unwrap();
        println!("id:{}",id3);
        bit_map.set(id3, true);

        bit_map.set(id1, false);
        println!("{:?}", bit_map);

        let id4 = bit_map.first_false_value().unwrap();
        println!("id:{}",id4);
        bit_map.set(id4, true);

        let id5 = bit_map.first_false_value().unwrap();
        println!("id:{}",id5);
        bit_map.set(id5, true);

        println!("{:?}", bit_map);
    }

    #[test]
    pub fn test_set() {
        let mask = u8::MAX - (1 << 7);
        println!("mask:{},{},{}",u8::MAX,mask,(1<<7) as u8);
    }
}